import os
import time
import json
import socket
import struct
import hashlib
import logging
import optparse
import traceback
import collections

from logging import critical as log


class g:
    kv = dict()
    kv_tmp = dict()
    acks = collections.deque()
    port = None
    peers = dict()
    followers = dict()
    quorum = 0
    state = ''
    reboot = 0
    seq = 0
    filenum = 0
    offset = 0
    checksum = ''
    vclock = dict()
    stats = None

    @classmethod
    def json(cls):
        g.seq += 1
        return json.dumps(dict(
            state=g.state,
            port=g.port,
            filenum=g.filenum,
            offset=g.offset,
            reboot=g.reboot,
            seq=g.seq,
            peers=dict([('{0}:{1}'.format(k[0], k[1]), dict(
                reboot=v['reboot'],
                seq=v['seq'],
                filenum=v['filenum'],
                offset=v['offset'],
                state=v['state']
                )) for k,v in g.peers.iteritems() if v]),
            vclock=g.vclock))


def sync_request(src, buf):
    return dict(msg='sync_response', buf=g.json())


def sync_response(src, buf):
    assert(src in g.peers)
    g.peers[src] = json.loads(buf)

    msgs = [dict(msg='sync_request')]

    if 'new-sync' == g.state:
        count = 0
        for k in filter(lambda k: g.peers[k], g.peers.keys()):
            if g.filenum != g.peers[k]['filenum']:
                continue

            if g.offset != g.peers[k]['offset']:
                continue

            count += 1

        if count >= g.quorum:
            log('quorum({0}) >= {1}) in sync with new session'.format(
                count, g.quorum))

            g.state = 'leader'
            log('WRITE enabled')

        return msgs


    if type(g.state) is tuple:
        return msgs

    if g.peers[src]['state'] in ('old-sync', 'new-sync', 'leader'):
        g.state = src
        log('identified current leader{0}'.format(src))
    elif not g.peers[src]['state']:
        if g.peers[src]['filenum'] == g.filenum:
            peer_vclock = g.peers[src]['vclock']
            for key in set(peer_vclock).intersection(set(g.vclock)):
                if peer_vclock[key] > g.vclock[key]:
                    os.remove(os.path.join(opt.data, str(gfilenum)))
                    log(('exiting after removing file({0}) as vclock({1}) '
                         'is ahead'.format(g.filenum, src)))
                    os._exit(0)

        leader = (g.port, g.__dict__, 'self')
        count = 0
        for k, v in g.peers.iteritems():
            if v:
                count += 1
                l, p = leader[1], v

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
            g.state = src
            log('leader({0}) selected due to {1}'.format(src, leader[2]))

    if type(g.state) is tuple:
        msgs.append(dict(msg='replication_request', buf=g.json()))

        log('LEADER{0} identified'.format(src))
        log('sent replication-request to{0} file({1}) offset({2})'.format(
            src, g.filenum, g.offset))

    return msgs


def replication_request(src, buf):
    req = json.loads(buf)

    log('received replication-request from{0} file({1}) offset({2})'.format(
        src, req['filenum'], req['offset']))

    if type(g.state) is tuple:
        log('rejecting replication-request from{0} as following'.format(
            src, g.state))
        raise Exception('reject-replication-request')

    if src not in g.followers:
        log('accepted {0} as follower({1})'.format(src, len(g.followers)))

    g.followers[src] = req
    if tuple(req['port']) in g.peers:
        g.peers[tuple(req['port'])] = req

    if not g.state and len(g.followers) == g.quorum:
        g.state = 'old-sync'
        log('assuming LEADERSHIP as quorum reached({0})'.format(g.quorum))

    if 'old-sync' == g.state:
        count = 0
        for src in filter(lambda k: g.followers[k], g.followers.keys()):
            if g.filenum != g.followers[src]['filenum']:
                continue

            if g.offset != g.followers[src]['offset']:
                continue

            count += 1

        if count >= g.quorum:
            log('quorum({0} >= {1}) in sync with old session'.format(
                count, g.quorum))

            g.filenum += 1
            g.offset = 0

            vclk = {'{0}:{1}'.format(g.port[0], g.port[1]): (g.reboot, g.seq)}
            for k in filter(lambda k: g.peers[k], g.peers):
                vclk['{0}:{1}'.format(k[0], k[1])] = (g.peers[k]['reboot'],
                                                      g.peers[k]['seq'])
            vclk = json.dumps(vclk)

            append(vclk)
            g.state = 'new-sync'
            log('new leader SESSION({0}) VCLK{1}'.format(g.filenum, vclk))

    responses = list()
    if 'leader' == g.state:
        offsets = list()
        for k, v in g.peers.iteritems():
            if v and v['filenum'] == g.filenum:
                offsets.append(v['offset'])

        if len(offsets) >= g.quorum:
            committed_offset = sorted(offsets, reverse=True)[g.quorum-1]

            while g.acks:
                if g.acks[0][0] > committed_offset:
                    break

                ack = g.acks.popleft()
                responses.append(ack[1])

    return responses + get_replication_responses()


def get_replication_responses():
    msgs = list()
    for src in filter(lambda k: g.followers[k], g.followers.keys()):
        req = g.followers[src]

        if 0 == req['filenum']:
            f = map(int, filter(lambda x: x != 'reboot', os.listdir(opt.data)))
            if f:
                log('sent replication-nextfile to {0} file({1})'.format(
                    src, min(f)))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(opt.data, str(req['filenum']))):
            continue

        with open(os.path.join(opt.data, str(req['filenum']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['offset']:
                log(('sent replication-truncate to {0} file({1}) offset({2}) '
                     'truncate({3})').format(
                    src, req['filenum'], req['offset'], fd.tell()))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_truncate',
                                 buf=json.dumps(dict(truncate=fd.tell()))))

            if fd.tell() == req['offset']:
                if g.filenum == req['filenum']:
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

    f = os.open(os.path.join(opt.data, str(g.filenum)), os.O_RDWR)
    n = os.fstat(f).st_size
    os.ftruncate(f, req['truncate'])
    os.fsync(f)
    os.close(f)

    log('file({0}) truncated({1}) original({2})'.format(
        g.filenum, req['truncate'], n))
    os._exit(0)


def replication_nextfile(src, buf):
    req = json.loads(buf)

    log('received replication-nextfile from{0}'.format(src))

    g.filenum = req['filenum']
    g.offset = 0

    log('sent replication-request to{0} file({1}) offset({2})'.format(
        src, g.filenum, g.offset))
    return dict(msg='replication_request', buf=g.json())


def replication_response(src, buf):
    log(('received replication-response from {0} size({1})').format(
        src, len(buf)))

    assert(src == g.state)
    assert(len(buf) > 0)

    try:
        f = os.open(os.path.join(opt.data, str(g.filenum)),
                    os.O_CREAT | os.O_WRONLY | os.O_APPEND)

        assert(g.offset == os.fstat(f).st_size)
        assert(len(buf) == os.write(f, buf))
        assert(g.offset+len(buf) == os.fstat(f).st_size)

        os.fsync(f)
        os.close(f)

        g.__dict__.update(scan(opt.data, g.filenum, g.offset, g.checksum,
                       index_put))

        log('sent replication-request to{0} file({1}) offset({2})'.format(
            src, g.filenum, g.offset))
        return dict(msg='replication_request', buf=g.json())
    except:
        traceback.print_exc()
        os._exit(0)


def on_connect(src):
    log('connected to {0}'.format(src))
    return dict(msg='sync_request')


def on_disconnect(src, exc, tb):
    if exc:
        log(tb)

    g.peers[src] = None
    if src == g.state:
        g.state = None
        g.followers = dict()
        log('NO LEADER as {0} disconnected'.format(src))


def on_accept(src):
    log('accepted connection from {0}'.format(src))


def on_reject(src, exc, tb):
    if exc:
        log(tb)

    log('terminated connection from {0}'.format(src))

    if src in g.followers:
        g.followers.pop(src)
        log('removed follower{0} remaining({1})'.format(src, len(g.followers)))

    if g.state in ('old-sync', 'new-sync', 'leader'):
        if len(g.followers) < g.quorum:
            log('exiting as followers({0}) < quorum({1})'.format(
                len(g.followers), g.quorum))
            os._exit(0)


def on_stats(stats):
    g.stats = stats


def state(src, buf):
    return dict(buf=g.json())


def put(src, buf):
    try:
        i = 0
        buf_list = list()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            filenum = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            offset = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
            value_len = struct.unpack('!Q', buf[i+24+key_len:i+32+key_len])[0]
            value = buf[i+32+key_len:i+32+key_len+value_len]

            f, o, v = g.kv.get(key, g.kv_tmp.get(key, (0, 0, '')))
            assert((f == filenum) and (o == offset)), 'version mismatch'

            buf_list.append(struct.pack('!Q', key_len))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', value_len))
            buf_list.append(value)

            i += 32 + key_len + value_len

        assert(i == len(buf)), 'invalid put request'

        if g.offset > opt.max:
            log('exiting as max size reached({0} > {1})'.format(
                g.offset, opt.max))
            os._exit(0)

        append(''.join(buf_list))

        ack = dict(dst=src, buf=struct.pack('!B', 0) + 'ok')
        responses = get_replication_responses()
        if responses:
            g.acks.append((g.offset, ack))
            return responses
        else:
            return ack
    except:
        return dict(buf=struct.pack('!B', 1) + traceback.format_exc())


def append(buf):
    chksum = hashlib.sha1(g.checksum.decode('hex'))
    chksum.update(buf)

    with open(os.path.join(opt.data, str(g.filenum)), 'ab', 0) as fd:
        fd.write(struct.pack('!Q', len(buf)) + buf + chksum.digest())

    g.__dict__.update(scan(opt.data, g.filenum, g.offset, g.checksum,
                      index_put))


def get(src, buf):
    i = 0
    result = list()
    while i < len(buf):
        key_len = struct.unpack('!Q', buf[i:i+8])[0]
        key = buf[i+8:i+8+key_len]
        f, o, v = g.kv.get(key, (0, 0, ''))

        result.append(struct.pack('!Q', key_len))
        result.append(key)
        result.append(struct.pack('!Q', f))
        result.append(struct.pack('!Q', o))
        result.append(struct.pack('!Q', len(v)))
        result.append(v)

        i += 8 + key_len

    return dict(buf=''.join(result))


def index_put(key, filenum, offset, value):
    g.kv[key] = (filenum, offset, value)


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
                            key_len = struct.unpack('!Q', x[i:i+8])[0]
                            key = x[i+8:i+8+key_len]
                            value_len = struct.unpack(
                                '!Q',
                                x[i+8+key_len:i+16+key_len])[0]
                            value = x[i+16+key_len:i+16+key_len+value_len]

                            ofst = offset+i+16+key_len
                            i += 16 + key_len + value_len

                            if callback:
                                callback(key, filenum, ofst, value)
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
    global opt

    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='string',
                      help='server:port tuple. skip to start the client')
    parser.add_option('--peers', dest='peers', type='string',
                      help='comma separated list of ip:port')
    parser.add_option('--data', dest='data', type='string',
                      help='data directory', default='data')
    parser.add_option('--max', dest='max', type='int',
                      help='max file size', default='256')
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

    if not os.path.isdir(opt.data):
        os.mkdir(opt.data)

    try:
        with open(os.path.join(opt.data, 'reboot')) as fd:
            g.reboot = int(fd.read()) + 1
    except:
        g.reboot = 1
    finally:
        with open(os.path.join(opt.data, 'tmp'), 'w') as fd:
            fd.write(str(g.reboot))
        os.rename(os.path.join(opt.data, 'tmp'),
                  os.path.join(opt.data, 'reboot'))
        log('RESTARTING sequence({0})'.format(g.reboot))

    files = map(int, filter(lambda x: x != 'reboot', os.listdir(opt.data)))
    if files:
        with open(os.path.join(opt.data, str(min(files))), 'rb') as fd:
            vclklen = struct.unpack('!Q', fd.read(8))[0]
            g.filenum = min(files)
            g.vclock = json.loads(fd.read(vclklen))
            g.checksum = fd.read(20).encode('hex')
            g.offset = fd.tell()
            assert(g.offset == vclklen + 28)

        g.__dict__.update(scan(opt.data, g.filenum, g.offset, g.checksum,
                               index_put))
        assert(g.filenum == max(files))

        f = os.open(os.path.join(opt.data, str(g.filenum)), os.O_RDWR)
        n = os.fstat(f).st_size
        if n > g.offset:
            os.ftruncate(f, g.offset)
            os.fsync(f)
            log('file({0}) truncated({1}) original({2})'.format(
                g.filenum, g.offset, n))
        os.close(f)

    return dict(cert=opt.cert, port=g.port, peers=peers)
