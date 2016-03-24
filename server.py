import os
import time
import json
import socket
import struct
import random
import signal
import hashlib
import logging
import hashlib
import optparse
import traceback
import collections

from logging import critical as log

opt = None


class g:
    fd = None
    kv = dict()
    kv_tmp = dict()
    key_list = collections.deque()
    vclocks = dict()
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
    size = 0
    checksum = ''
    stats = None

    @classmethod
    def json(cls):
        return json.dumps(dict(
            state=g.state,
            port=g.port,
            filenum=g.filenum,
            offset=g.offset,
            size=g.size,
            reboot=g.reboot,
            seq=g.seq,
            keys=[len(g.key_list), len(g.kv), len(g.kv_tmp), len(g.acks)],
            peers=dict([('{0}:{1}'.format(k[0], k[1]), dict(
                reboot=v['reboot'],
                seq=v['seq'],
                filenum=v['filenum'],
                offset=v['offset'],
                state=v['state'])) for k, v in g.peers.iteritems() if v]),
            vclock=g.vclocks.get(g.filenum, dict())))

    @classmethod
    def update(cls, d):
        cls.__dict__.update(d)


def sync_request(src, buf):
    g.seq += 1
    return dict(msg='sync_response', buf=g.json())


def sync_response(src, buf):
    assert(src in g.peers)
    g.peers[src] = json.loads(buf)

    msgs = [dict(msg='sync_request')]

    if type(g.state) is tuple or g.state in ('old-sync', 'leader'):
        return msgs

    if 'new-sync' == g.state:
        in_sync = filter(lambda k: g.filenum == g.peers[k]['filenum'],
                         filter(lambda k: g.offset == g.peers[k]['offset'],
                                filter(lambda k: g.peers[k], g.peers.keys())))

        if len(in_sync) >= g.quorum:
            log('quorum({0} >= {1}) in sync with new session'.format(
                len(in_sync), g.quorum))

            g.state = 'leader'
            log('WRITE enabled')

        return msgs

    if g.peers[src]['filenum'] == g.filenum:
        peer_vclock = g.peers[src]['vclock']
        my_vclock = g.vclocks.get(g.filenum, dict())

        for key in set(peer_vclock).intersection(set(my_vclock)):
            if peer_vclock[key] > my_vclock[key]:
                os.remove(os.path.join(opt.data, str(g.filenum)))
                log(('REMOVED file({0}) as vclock({1}) is ahead'.format(
                    g.filenum, src)))
                os._exit(0)

    if g.peers[src]['state'] in ('old-sync', 'new-sync', 'leader'):
        g.state = src
        log('LEADER({0}) identified'.format(src))
    else:
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

                h = hashlib.md5(str(k)+str(g.filenum)).hexdigest()
                if h > hashlib.md5(str(leader[0])+str(g.filenum)).hexdigest():
                    leader = (k, p, 'round robin selection')

        if (src == leader[0]) and (count >= g.quorum):
            g.state = src
            log('LEADER({0}) selected due to {1}'.format(src, leader[2]))

    if type(g.state) is tuple:
        msgs.append(dict(msg='replication_request', buf=g.json()))

        log('sent replication-request to{0} file({1}) offset({2})'.format(
            src, g.filenum, g.size))

    return msgs


def replication_request(src, buf):
    req = json.loads(buf)

    log('received replication-request from{0} file({1}) offset({2})'.format(
        src, req['filenum'], req['offset']))

    if type(g.state) is tuple:
        log('rejecting replication-request from{0} as following{1}'.format(
            src, g.state))
        raise Exception('reject-replication-request')

    if src not in g.followers:
        log('accepted {0} as follower({1})'.format(src, len(g.followers)+1))

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
            os.fsync(g.fd)
            g.state = 'new-sync'
            log('new leader SESSION({0}) VCLK{1}'.format(g.filenum, vclk))
            if g.seq % 2:
                log('exiting to force test vclock conflict')
                os._exit(0)

    acks = list()
    committed_offset = 0
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
                for key in ack[2]:
                   g.kv[key] = g.kv_tmp.pop(key)
                   g.key_list.append((key, g.kv[key][0], g.kv[key][1]))
                acks.append(ack[1])

    if acks:
        os.fsync(g.fd)
    elif committed_offset == g.offset and g.offset > opt.max:
        log('max file size reached({0} > {1})'.format(g.offset, opt.max))
        os._exit(0)

    return acks + get_replication_responses()


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

        v1 = json.dumps(g.vclocks[req['filenum']], sort_keys=True)
        v2 = json.dumps(req['vclock'], sort_keys=True)
        if req['vclock'] and v1 != v2:
            log('vclock mismatch src{0} file({1})'.format(src, req['filenum']))
            log('local vclock {0}'.format(v1))
            log('peer vclock {0}'.format(v2))

            log(('sent replication-truncate to{0} file({1}) offset({2}) '
                 'truncate(0)').format(src, req['filenum'], req['size']))

            g.followers[src] = None
            msgs.append(dict(dst=src, msg='replication_truncate',
                             buf=json.dumps(dict(truncate=0))))

        with open(os.path.join(opt.data, str(req['filenum']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['size']:
                log(('sent replication-truncate to{0} file({1}) offset({2}) '
                     'truncate({3})').format(
                    src, req['filenum'], req['size'], fd.tell()))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_truncate',
                                 buf=json.dumps(dict(truncate=fd.tell()))))

            if fd.tell() == req['size']:
                if g.filenum == req['filenum']:
                    continue

                log('sent replication-nextfile to{0} file({1})'.format(
                    src, req['filenum']+1))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                            buf=json.dumps(dict(filenum=req['filenum']+1))))

            fd.seek(req['size'])
            buf = fd.read(100*2**20)
            if buf:
                log(('sent replication-response to{0} file({1}) offset({2}) '
                     'size({3})').format(
                    src, req['size'], req['offset'], len(buf)))

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

    log('received replication-nextfile from{0} filenum({1})'.format(
        src, req['filenum']))

    g.filenum = req['filenum']
    g.offset = 0
    g.size = 0

    if g.fd:
        os.fsync(g.fd)
        os.close(g.fd)
        g.fd = None

    log('sent replication-request to{0} file({1}) offset({2})'.format(
        src, g.filenum, g.size))
    return dict(msg='replication_request', buf=g.json())


def replication_response(src, buf):
    log(('received replication-response from {0} size({1})').format(
        src, len(buf)))

    try:
        assert(src == g.state)
        assert(len(buf) > 0)

        if not g.fd:
            g.fd = os.open(os.path.join(opt.data, str(g.filenum)),
                           os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                           0644)

        assert(g.size == os.fstat(g.fd).st_size)
        assert(len(buf) == os.write(g.fd, buf))
        os.fsync(g.fd)
        assert(g.offset+len(buf) == os.fstat(g.fd).st_size)

        g.update(scan(opt.data, g.filenum, g.offset, g.checksum,
                      kv_put, vclock_put))

        log('sent replication-request to{0} file({1}) offset({2})'.format(
            src, g.filenum, g.size))
        return dict(msg='replication_request', buf=g.json())
    except:
        traceback.print_exc()
        os._exit(0)


def on_connect(src):
    return dict(msg='sync_request')


def on_disconnect(src, exc, tb):
    if exc:
        log(tb)

    g.peers[src] = None
    if src == g.state:
        assert(not g.followers)

        g.state = None
        if g.fd:
            os.fsync(g.fd)
            os.close(g.fd)
            g.fd = None
        log('')
        log('NO LEADER as {0} disconnected'.format(src))


def on_accept(src):
    pass


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
    if g.offset > opt.max:
        return

    try:
        i = 0
        buf_list = list()
        keys = list()
        update_count = 0
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            keys.append(key)
            filenum = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            offset = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
            value_len = struct.unpack('!Q', buf[i+24+key_len:i+32+key_len])[0]
            value = buf[i+32+key_len:i+32+key_len+value_len]

            f, o, v = g.kv.get(key, g.kv_tmp.get(key, (0, 0, '')))
            assert((f == filenum) and (o == offset)), 'version mismatch'
            if f > 0:
                update_count += 1

            buf_list.append(struct.pack('!Q', key_len))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', value_len))
            buf_list.append(value)

            i += 32 + key_len + value_len

        assert(i == len(buf)), 'invalid put request'

        updated_keys = list()
        while update_count > 0:
            if g.key_list[0][1] == g.filenum:
                break

            k, f, o = g.key_list.popleft()
            if k in g.kv and k not in keys:
                if f == g.kv[k][0] and o == g.kv[k][1]:
                    buf_list.append(struct.pack('!Q', len(k)))
                    buf_list.append(k)
                    buf_list.append(struct.pack('!Q', len(g.kv[k][2])))
                    buf_list.append(g.kv[k][2])
                    updated_keys.append(k)
                    update_count -= 1

        append(''.join(buf_list))

        buf_list = [struct.pack('!B', 0)]
        for key in keys:
            filenum, offset, _ = g.kv_tmp[key]
            buf_list.append(struct.pack('!Q', len(key)))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', filenum))
            buf_list.append(struct.pack('!Q', offset))

        keys.extend(updated_keys)
        g.acks.append((g.offset, dict(dst=src, buf=''.join(buf_list)), keys))
        return get_replication_responses()
    except:
        return dict(buf=struct.pack('!B', 1) + traceback.format_exc())


def append(buf):
    chksum = hashlib.sha1(g.checksum.decode('hex'))
    chksum.update(buf)

    if not g.fd:
        g.fd = os.open(os.path.join(opt.data, str(g.filenum)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    os.write(g.fd, struct.pack('!Q', len(buf)) + buf + chksum.digest())

    g.update(scan(opt.data, g.filenum, g.offset, g.checksum,
                  kv_tmp_put, vclock_put))


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


def kv_put(key, filenum, offset, value):
    g.kv[key] = (filenum, offset, value)
    g.key_list.append((key, filenum, offset))


def kv_tmp_put(key, filenum, offset, value):
    g.kv_tmp[key] = (filenum, offset, value)


def vclock_put(filenum, vclock):
    g.vclocks[filenum] = vclock


def scan(path, filenum, offset, checksum, callback_kv, callback_vclock):
    start_time = time.time()
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

                            callback_kv(key, filenum, ofst, value)
                    else:
                        callback_vclock(filenum, json.loads(x))

                    offset += len(x) + 28
                    assert(offset <= total_size)
                    assert(offset == fd.tell())

                    result.update(dict(
                        filenum=filenum,
                        offset=offset,
                        size=total_size,
                        checksum=checksum.encode('hex')))

                    if time.time() > start_time + 10:
                        log(('scanned file({filenum}) offset({offset}) '
                             'size({size})').format(**result))
                        start_time = time.time()

            filenum += 1
            offset = 0
        except:
            if 'filenum' in result:
                log(('scanned file({filenum}) offset({offset}) '
                     'size({size})').format(**result))
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
    parser.add_option('--timeout', dest='timeout', type='int',
                      help='timeout in seconds', default='30')
    parser.add_option('--cert', dest='cert', type='string',
                      help='certificate file path', default='cert.pem')
    parser.add_option('--log', dest='log', type=int,
                      help='logging level', default=logging.INFO)
    opt, args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s: %(message)s', level=opt.log)

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
        g.filenum = min(files)
        with open(os.path.join(opt.data, str(g.filenum)), 'rb') as fd:
            vclklen = struct.unpack('!Q', fd.read(8))[0]
            g.vclocks[g.filenum] = json.loads(fd.read(vclklen))
            g.checksum = fd.read(20).encode('hex')
            g.offset = fd.tell()
            fd.seek(0, 2)
            g.size = fd.tell()
            assert(g.offset == vclklen + 28)

        g.__dict__.update(scan(opt.data, g.filenum, g.offset, g.checksum,
                               kv_put, vclock_put))

        if g.kv:
            for n in range(min(files), min([g.kv[k][0] for k in g.kv])):
                os.remove(os.path.join(opt.data, str(n)))
                log('removed file({0})'.format(n))

        f = os.open(os.path.join(opt.data, str(g.filenum)), os.O_RDWR)
        n = os.fstat(f).st_size
        if n > g.offset:
            os.ftruncate(f, g.offset)
            os.fsync(f)
            log('file({0}) truncated({1}) original({2})'.format(
                g.filenum, g.offset, n))
            os.close(f)

        filenum = g.filenum + 1
        while True:
            try:
                os.remove(os.path.join(opt.data, str(filenum)))
                log('removed file({0})'.format(filenum))
                filenum += 1
            except:
                break

    signal.alarm(random.randint(opt.timeout, 2*opt.timeout))

    return dict(cert=opt.cert, port=g.port, peers=peers)
