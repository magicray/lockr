import os
import time
import json
import socket
import struct
import hashlib
import logging
import optparse
import traceback

from logging import critical as log


class g:
    max_file_size = 256
    data = 'data'
    port = None
    peers = None
    quorum = None
    leader = None
    session = None
    role = None
    clock = 0
    followers = dict()
    state = dict(filenum=0, offset=0, checksum='', clock=0, vclock=None)
    docs = dict()
    stats = None


def sync_request(src, buf):
    if g.leader and 'self' != g.leader and src in g.followers:
        log(('disconnecting follower{0} as already following{1}').format(
            src, g.leader))
        raise Exception('kill-follower')

    return dict(msg='sync_response', buf=json.dumps(dict(
        state=g.state,
        leader=g.leader,
        stats=g.stats,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))))


def sync_response(src, buf):
    assert(src in g.peers)
    g.peers[src] = json.loads(buf)

    msgs = [dict(msg='sync_request')]

    if g.leader:
        return msgs

    if 'self' == g.peers[src]['leader']:
        g.leader = src
        log('identified current leader{0}'.format(src))
    elif g.peers[src]['leader'] is None:
        if g.peers[src]['state']['filenum'] == g.state['filenum']:
            peer_vclock = g.peers[src]['state']['vclock']
            my_vclock = g.state['vclock']
            for key in set(peer_vclock).intersection(set(my_vclock)):
                if peer_vclock[key] > my_vclock[key]:
                    os.remove(os.path.join(g.data, str(g.state['filenum'])))
                    log(('exiting after removing file({0}) as vclock({1}) '
                         'is ahead'.format(g.state['filenum'], src)))
                    exit(0)

        leader = (g.port, g.state, 'self')
        count = 0
        for k, v in g.peers.iteritems():
            if v:
                count += 1
                l, p = leader[1], v['state']

                if l['filenum'] != p['filenum']:
                    if l['filenum'] < p['filenum']:
                        leader = (k, p, 'filenum({0} > {1})'.format(
                            p['filenum'], l['filenum']))
                    continue

                if l['offset'] != p['offset']:
                    if l['offset'] < p['offset']:
                        leader = (k, p, 'offset({0} > {1})'.format(
                            p['offset'], l['offset']))
                    continue

                if k > leader[0]:
                    leader = (k, p, 'address({0}:{1} > {2}:{3})'.format(
                        k[0], k[1], leader[0][0], leader[0][1]))

        if (src == leader[0]) and (count >= g.quorum):
            g.leader = src
            log('leader({0}) selected due to {1}'.format(src, leader[2]))

    if g.leader:
        msgs.append(dict(msg='replication_request', buf=json.dumps(g.state)))

        log('LEADER{0} identified'.format(src))
        log('sent replication-request to{0} file({1}) offset({2})'.format(
            src, g.state['filenum'], g.state['offset']))

    return msgs


def replication_request(src, buf):
    req = json.loads(buf)

    log('received replication-request from{0} file({1}) offset({2})'.format(
        src, req['filenum'], req['offset']))

    if g.leader and 'self' != g.leader:
        log(('rejecting replication-request from{0} as already '
             'following{1}').format(src, g.leader))
        raise Exception('reject-replication-request')

    g.followers[src] = req
    log('accepted {0} as follower({1})'.format(src, len(g.followers)))

    if 'self' != g.leader and len(g.followers) == g.quorum:
        g.leader = 'self'
        log('assuming LEADERSHIP as quorum reached({0})'.format(g.quorum))

    if 'self' != g.leader:
        return

    if not g.session:
        count = 0
        for src in filter(lambda k: g.followers[k], g.followers.keys()):
            if g.state['filenum'] != g.followers[src]['filenum']:
                continue

            if g.state['offset'] != g.followers[src]['offset']:
                continue

            count += 1

        if count >= g.quorum:
            log('quorum({0} >= {1}) in sync with old session'.format(
                count, g.quorum))

            g.state['filenum'] += 1
            g.state['offset'] = 0
            g.session = g.state['filenum']

            vclk = {'{0}:{1}'.format(g.port[0], g.port[1]): g.state['clock']}
            for k in filter(lambda k: g.peers[k], g.peers):
                vclk['{0}:{1}'.format(k[0], k[1])] = g.peers[k]['state'][
                    'clock']
            vclk = json.dumps(vclk)

            append(vclk)
            g.session = g.state['filenum']
            log('new leader SESSION({0}) VCLK{1}'.format(g.session, vclk))

    if ('leader' == g.role) and g.session:
        assert(g.session == g.state['filenum'])

        count = 0
        for k in filter(lambda k: g.peers[k], g.peers.keys()):
            if g.state['filenum'] != g.peers[k]['state']['filenum']:
                continue

            if g.state['offset'] != g.peers[k]['state']['offset']:
                continue

            count += 1

        if count >= g.quorum:
            log('quorum({0}) >= {1}) in sync with new session'.format(
                count, g.quorum))
            g.role = 'leader'
            log('write requests enabled')

    return send_replication_responses()


def send_replication_responses():
    msgs = list()
    for src in filter(lambda k: g.followers[k], g.followers.keys()):
        req = g.followers[src]

        if 0 == req['filenum']:
            f = map(int, filter(lambda x: x != 'clock', os.listdir(g.data)))
            if f:
                log('sent replication-nextfile to {0} file({1})'.format(
                    src, min(f)))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(g.data, str(req['filenum']))):
            continue

        with open(os.path.join(g.data, str(req['filenum']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['offset']:
                log(('sent replication-truncate to {0} file({1}) offset({2}) '
                     'truncate({3})').format(
                    src, req['filenum'], req['offset'], fd.tell()))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_truncate',
                                 buf=json.dumps(dict(truncate=fd.tell()))))

            if fd.tell() == req['offset']:
                if g.state['filenum'] == req['filenum']:
                    continue

                log('sent replication-nextfile to {0} file({1})'.format(
                    src, req['filenum']+1))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                            buf=json.dumps(dict(filenum=req['filenum']+1))))

            fd.seek(req['offset'])
            buf = fd.read(100*2**20)
            if buf:
                log(('sent replication-response to {0} file({1}) offset({2}) '
                     'size({3})').format(
                    src, req['filenum'], req['offset'], len(buf)))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_response', buf=buf))

    return msgs


def replication_truncate(src, buf):
    req = json.loads(buf)

    log('received replication-truncate from{0} size({1})'.format(
        src, req['truncate']))

    f = os.open(os.path.join(g.data, str(g.state['filenum'])), os.O_RDWR)
    n = os.fstat(f).st_size
    os.ftruncate(f, req['truncate'])
    os.fsync(f)
    os.close(f)

    log('file({0}) truncated({1}) original({2})'.format(
        g.state['filenum'], req['truncate'], n))
    exit(0)


def replication_nextfile(src, buf):
    req = json.loads(buf)

    log('received replication-nextfile from{0}'.format(src))

    g.state['filenum'] = req['filenum']
    g.state['offset'] = 0

    log('sent replication-request to{0} file({1}) offset({2})'.format(
        src, g.state['filenum'], g.state['offset']))
    return dict(msg='replication_request', buf=json.dumps(g.state))


def replication_response(src, buf):
    log(('received replication-response from {0} size({1})').format(
        src, len(buf)))

    assert(src == g.leader)
    assert(len(buf) > 0)

    try:
        f = os.open(os.path.join(g.data, str(g.state['filenum'])),
                    os.O_CREAT | os.O_WRONLY | os.O_APPEND)

        assert(g.state['offset'] == os.fstat(f).st_size)
        assert(len(buf) == os.write(f, buf))
        assert(g.state['offset']+len(buf) == os.fstat(f).st_size)

        os.fsync(f)
        os.close(f)

        g.state.update(scan(g.data, g.state['filenum'], g.state['offset'],
                            g.state['checksum'], index_put))

        log('sent replication-request to{0} file({1}) offset({2})'.format(
            src, g.state['filenum'], g.state['offset']))
        return dict(msg='replication_request', buf=json.dumps(g.state))
    except:
        traceback.print_exc()
        exit(0)


def on_connect(src):
    log('connected to {0}'.format(src))
    return dict(msg='sync_request')


def on_disconnect(src, reason):
    # log(reason)
    g.peers[src] = None
    if src == g.leader:
        g.leader = None
        log('NO LEADER as {0} disconnected'.format(src))


def on_accept(src):
    log('accepted connection from {0}'.format(src))


def on_reject(src, reason):
    # log(reason)
    log('terminated connection from {0}'.format(src))

    if src in g.followers:
        g.followers.pop(src)
        log('removed follower{0} remaining({1})'.format(src, len(g.followers)))

    if ('self' == g.leader) and (len(g.followers) < g.quorum):
        log('relinquishing leadership as followers({0}) < quorum({1})'.format(
            len(g.followers), g.quorum))
        exit(0)


def on_stats(stats):
    g.stats = stats


def state(src, buf):
    state = dict([('{0}:{1}'.format(*k), v) for k, v in g.peers.iteritems()])

    state['self'] = dict(
        state=g.state,
        leader=g.leader,
        role=g.role,
        stats=g.stats,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))

    return dict(buf=json.dumps(state))


def put(src, buf):
    try:
        i = 0
        buf_list = list()
        while i < len(buf):
            key = buf[i:i+32]
            filenum = struct.unpack('!Q', buf[i+32:i+40])[0]
            offset = struct.unpack('!Q', buf[i+40:i+48])[0]
            length = struct.unpack('!Q', buf[i+48:i+56])[0]

            f, o, l = g.docs.get(key, (0, 0, 0))
            assert((f == filenum) and (o == offset)), 'version mismatch'

            buf_list.append(key)
            buf_list.append(struct.pack('!Q', length))
            buf_list.append(buf[i+56:i+56+length])

            i += 56 + length

        assert(i == len(buf)), 'invalid put request'

        if g.state['offset'] > g.max_file_size:
            log('exiting as max size reached({0} > {1})'.format(
                g.state['offset'], g.max_file_size))
            exit(0)

        append(''.join(buf_list))
        return dict(buf=struct.pack('!B', 0) + 'ok')
    except:
        return dict(buf=struct.pack('!B', 1) + traceback.format_exc())


def append(buf):
    chksum = hashlib.sha1(g.state['checksum'].decode('hex'))
    chksum.update(buf)

    with open(os.path.join(g.data, str(g.state['filenum'])), 'ab', 0) as fd:
        fd.write(struct.pack('!Q', len(buf)) + buf + chksum.digest())

    g.state.update(scan(g.data, g.state['filenum'], g.state['offset'],
                        g.state['checksum'], index_put))


def get(src, buf):
    i = 0
    result = list()
    while i < len(buf):
        key = buf[i:i+32]
        f, o, l = g.docs.get(key, (0, 0, 0))
        result.append(key)
        result.append(struct.pack('!Q', f))
        result.append(struct.pack('!Q', o))
        result.append(struct.pack('!Q', l))
        if l > 0:
            with open(os.path.join(g.data, str(f)), 'rb') as fd:
                fd.seek(o)
                result.append(fd.read(l))
        i += 32

    return dict(buf=''.join(result))


def index_put(key, filenum, offset, length):
    g.docs[key] = (filenum, offset, length)


def scan(path, filenum, offset, checksum, callback=None):
    result = dict()
    checksum = checksum.decode('hex')
    while True:
        try:
            with open(os.path.join(path, str(filenum)), 'rb') as fd:
                fd.seek(0, 2)
                total_size = fd.tell()
                fd.seek(offset)

                while(offset < total_size):
                    x = fd.read(struct.unpack('!Q', fd.read(8))[0])
                    y = fd.read(20)

                    chksum = hashlib.sha1(checksum)
                    chksum.update(x)
                    assert(y == chksum.digest())
                    checksum = y

                    if offset > 0:
                        i = 0
                        while i < len(x):
                            key = x[i:i+32]
                            length = struct.unpack('!Q', x[i+32:i+40])[0]
                            ofst = offset+8+i+40
                            i += 40 + length

                            if callback:
                                callback(key, filenum, ofst, length)
                    else:
                        result['vclock'] = json.loads(x)

                    offset += len(x) + 28
                    assert(offset <= total_size)
                    assert(offset == fd.tell())

                    result.update(dict(
                        filenum=filenum,
                        offset=offset,
                        size=total_size,
                        checksum=checksum.encode('hex')))

                    log(('scanned file({filenum}) offset({offset}) '
                         'size({size})').format(**result))

            filenum += 1
            offset = 0
        except:
            return result


def on_init():
    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='string',
                      help='server:port tuple. skip to start the client')
    parser.add_option('--peers', dest='peers', type='string',
                      help='comma separated list of ip:port')
    parser.add_option('--cert', dest='cert', type='string',
                      help='certificate file path', default='cert.pem')
    parser.add_option('--log', dest='log', type=int,
                      help='logging level', default=logging.INFO)
    opt, args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s: %(message)s', level=opt.log)

    peers = list()
    if opt.peers:
        peers = set(map(lambda x: (socket.gethostbyname(x[0]), int(x[1])),
                        map(lambda x: x.split(':'),
                            opt.peers.split(','))))
    g.port = (socket.gethostbyname(opt.port.split(':')[0]),
              int(opt.port.split(':')[1]))

    g.peers = dict((ip_port, None) for ip_port in peers)
    g.quorum = int(len(g.peers)/2.0 + 0.6)
    g.state['vclock'] = [0]*(len(peers)+1)

    if not os.path.isdir(g.data):
        os.mkdir(g.data)

    try:
        with open(os.path.join(g.data, 'clock')) as fd:
            g.state['clock'] = (int(fd.read()) + 1, g.clock)
    except:
        g.state['clock'] = (1, g.clock)
    finally:
        with open(os.path.join(g.data, 'tmp'), 'w') as fd:
            fd.write(str(g.state['clock'][0]))
        os.rename(os.path.join(g.data, 'tmp'), os.path.join(g.data, 'clock'))
        log('RESTARTING sequence({0})'.format(g.state['clock']))

    files = map(int, filter(lambda x: x != 'clock', os.listdir(g.data)))
    if files:
        with open(os.path.join(g.data, str(min(files))), 'rb') as fd:
            vclklen = struct.unpack('!Q', fd.read(8))[0]
            g.state['filenum'] = min(files)
            g.state['vclock'] = json.loads(fd.read(vclklen))
            g.state['checksum'] = fd.read(20).encode('hex')
            g.state['offset'] = fd.tell()
            assert(g.state['offset'] == vclklen + 28)

        g.state.update(scan(g.data, g.state['filenum'], g.state['offset'],
                            g.state['checksum'], index_put))
        assert(g.state['filenum'] == max(files))

        f = os.open(os.path.join(g.data, str(g.state['filenum'])), os.O_RDWR)
        n = os.fstat(f).st_size
        if n > g.state['offset']:
            os.ftruncate(f, g.state['offset'])
            os.fsync(f)
            log('file({0}) truncated({1}) original({2})'.format(
                g.state['filenum'], g.state['offset'], n))
        os.close(f)

    return dict(cert=opt.cert, port=g.port, peers=peers)
