import os
import time
import json
import struct
import random
import signal
import logging
import hashlib
import optparse
import traceback
import collections

from logging import critical as log


class g:
    opt = None
    fd = None
    kv = dict()
    kv_tmp = dict()
    key_list = collections.deque()
    vclocks = dict()
    acks = collections.deque()
    peers = dict()
    followers = dict()
    quorum = 0
    state = ''
    clock = (0, 0)
    minfile = 0
    maxfile = 0
    offset = 0
    size = 0
    checksum = ''
    stats = None

    @classmethod
    def json(cls):
        return json.dumps(dict(
            state=g.state,
            node=node,
            port=g.opt.port,
            minfile=g.minfile,
            maxfile=g.maxfile,
            offset=g.offset,
            size=g.size,
            clock=g.clock,
            keys=[len(g.key_list), len(g.kv), len(g.kv_tmp), len(g.acks)],
            peers=dict([(k, dict(
                keys=v['keys'],
                clock=v['clock'],
                minfile=v['minfile'],
                maxfile=v['maxfile'],
                offset=v['offset'],
                state=v['state'])) for k, v in g.peers.iteritems() if v]),
            vclock=g.vclocks.get(g.maxfile, dict())))

    @classmethod
    def update(cls, d):
        cls.__dict__.update(d)


def sync(src, buf):
    g.peers[src] = json.loads(buf)

    g.clock = (g.clock[0], g.clock[1]+1)
    msgs = [dict(msg='sync', buf=g.json())]

    if g.state.startswith('following-'):
        return msgs

    if g.state in ('old-sync', 'leader'):
        return msgs

    if 'new-sync' == g.state:
        in_sync = filter(lambda k: g.maxfile == g.peers[k]['maxfile'],
                         filter(lambda k: g.offset == g.peers[k]['offset'],
                                g.peers))

        if len(in_sync) >= g.quorum:
            log('quorum({0} >= {1}) in sync with new session'.format(
                len(in_sync), g.quorum))

            g.state = 'leader'
            log('WRITE enabled')

        return msgs

    if g.peers[src]['maxfile'] == g.maxfile:
        peer_vclock = g.peers[src]['vclock']
        my_vclock = g.vclocks.get(g.maxfile, dict())

        for key in set(peer_vclock).intersection(set(my_vclock)):
            if peer_vclock[key] > my_vclock[key]:
                os.remove(os.path.join(g.opt.data, str(g.maxfile)))
                log(('REMOVED file({0}) as vclock({1}) is ahead'.format(
                    g.maxfile, src)))
                os._exit(0)

    if g.maxfile > 0 and g.peers[src]['minfile'] > g.maxfile:
        log('src{0} minfile({1}) > maxfile({2})'.format(
            src, g.peers[src]['minfile'], g.maxfile))
        while True:
            try:
                os.remove(os.path.join(g.opt.data, str(g.maxfile)))
                log('removed file({0})'.format(g.maxfile))
                g.maxfile -= 1
            except:
                os._exit(0)
                break

    if g.peers[src]['state'] in ('old-sync', 'new-sync', 'leader'):
        g.state = 'following-' + src
        log('LEADER({0}) identified'.format(src))
    else:
        leader = (node, g.__dict__, 'self')
        count = 0
        for k, v in g.peers.iteritems():
            count += 1
            l, p = leader[1], v

            if l['maxfile'] != p['maxfile']:
                if l['maxfile'] < p['maxfile']:
                    leader = (k, p, 'maxfile({0} > {1})'.format(
                        p['maxfile'], l['maxfile']))
                continue

            if l['offset'] != p['offset']:
                if l['offset'] < p['offset']:
                    leader = (k, p, 'offset({0} > {1})'.format(
                        p['offset'], l['offset']))
                continue

            h = hashlib.md5(k+str(p['maxfile'])).hexdigest()
            if h > hashlib.md5(leader[0]+str(p['maxfile'])).hexdigest():
                leader = (k, p, 'round robin selection')

        if (src == leader[0]) and (count >= g.quorum):
            g.state = 'following-' + src
            log('LEADER({0}) selected due to {1}'.format(src, leader[2]))

    if g.state.startswith('following-'):
        msgs.append(dict(msg='replication_request', buf=g.json()))

        log('sent replication-request to({0}) file({1}) offset({2})'.format(
            src, g.maxfile, g.size))

    return msgs


def replication_request(src, buf):
    req = json.loads(buf)

    log('received replication-request from({0}) file({1}) offset({2})'.format(
        src, req['maxfile'], req['offset']))

    if g.state.startswith('following-'):
        log('rejecting replication-request from({0}) as ({1})'.format(
            src, g.state))
        raise Exception('reject-replication-request')

    if src not in g.followers:
        log('accepted {0} as follower({1})'.format(src, len(g.followers)+1))

    g.followers[src] = req
    if req['port'] in g.peers:
        g.peers[req['port']] = req

    if not g.state and len(g.followers) == g.quorum:
        g.state = 'old-sync'
        log('assuming LEADERSHIP as quorum reached({0})'.format(g.quorum))

    if 'old-sync' == g.state:
        count = 0
        for src in filter(lambda k: g.followers[k], g.followers.keys()):
            if g.maxfile != g.followers[src]['maxfile']:
                continue

            if g.offset != g.followers[src]['offset']:
                continue

            count += 1

        if count >= g.quorum:
            log('quorum({0} >= {1}) in sync with old session'.format(
                count, g.quorum))

            g.maxfile += 1
            g.offset = 0

            vclk = {node: g.clock}
            for k in filter(lambda k: g.peers[k], g.peers):
                vclk[k] = g.peers[k]['clock']

            print(vclk)
            vclk = json.dumps(vclk)

            append(vclk)
            os.fsync(g.fd)
            g.state = 'new-sync'
            log('new leader SESSION({0}) VCLK{1}'.format(g.maxfile, vclk))
            if g.clock[1] % 2:
                log('exiting to force test vclock conflict')
                os._exit(0)

    acks = list()
    committed_offset = 0
    if 'leader' == g.state:
        offsets = list()
        for k, v in g.peers.iteritems():
            if v and v['maxfile'] == g.maxfile:
                offsets.append(v['offset'])

        if len(offsets) >= g.quorum:
            committed_offset = sorted(offsets, reverse=True)[g.quorum-1]

            while g.acks:
                if g.acks[0][0] > committed_offset:
                    break

                ack = g.acks.popleft()
                for key in ack[2]:
                    f, t, v = g.kv_tmp.pop(key)
                    kv_put(dict(key=key, filenum=f, txn=t, value=v))
                acks.append(ack[1])

    if acks:
        os.fsync(g.fd)
    elif committed_offset == g.offset and g.offset > g.opt.max:
        log('max file size reached({0} > {1})'.format(g.offset, g.opt.max))
        os._exit(0)

    return acks + get_replication_responses()


def get_replication_responses():
    msgs = list()
    for src in filter(lambda k: g.followers[k], g.followers.keys()):
        req = g.followers[src]

        if 0 == req['maxfile']:
            f = map(int, filter(lambda x: x != 'reboot',
                                os.listdir(g.opt.data)))
            if f:
                log('sent replication-nextfile to {0} file({1})'.format(
                    src, min(f)))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(g.opt.data, str(req['maxfile']))):
            continue

        v1 = json.dumps(g.vclocks[req['maxfile']], sort_keys=True)
        v2 = json.dumps(req['vclock'], sort_keys=True)
        if req['vclock'] and v1 != v2:
            log('vclock mismatch src{0} file({1})'.format(src, req['maxfile']))
            log('local vclock {0}'.format(v1))
            log('peer vclock {0}'.format(v2))

            log(('sent replication-truncate to{0} file({1}) offset({2}) '
                 'truncate(0)').format(src, req['maxfile'], req['size']))

            g.followers[src] = None
            msgs.append(dict(dst=src, msg='replication_truncate',
                             buf=json.dumps(dict(truncate=0))))

        with open(os.path.join(g.opt.data, str(req['maxfile']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['size']:
                log(('sent replication-truncate to{0} file({1}) offset({2}) '
                     'truncate({3})').format(
                    src, req['maxfile'], req['size'], fd.tell()))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_truncate',
                                 buf=json.dumps(dict(truncate=fd.tell()))))

            if fd.tell() == req['size']:
                if g.maxfile == req['maxfile']:
                    continue

                log('sent replication-nextfile to{0} file({1})'.format(
                    src, req['maxfile']+1))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                            buf=json.dumps(dict(filenum=req['maxfile']+1))))

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

    f = os.open(os.path.join(g.opt.data, str(g.maxfile)), os.O_RDWR)
    n = os.fstat(f).st_size
    os.ftruncate(f, req['truncate'])
    os.fsync(f)
    os.close(f)

    log('file({0}) truncated({1}) original({2})'.format(
        g.maxfile, req['truncate'], n))
    os._exit(0)


def replication_nextfile(src, buf):
    req = json.loads(buf)

    log('received replication-nextfile from({0}) filenum({1})'.format(
        src, req['filenum']))

    g.maxfile = req['filenum']
    g.offset = 0
    g.size = 0

    if g.fd:
        os.fsync(g.fd)
        os.close(g.fd)
        g.fd = None

    log('sent replication-request to({0}) file({1}) offset({2})'.format(
        src, g.maxfile, g.size))
    return dict(msg='replication_request', buf=g.json())


def replication_response(src, buf):
    log(('received replication-response from({0}) size({1})').format(
        src, len(buf)))

    try:
        assert(src == g.state.split('-')[1])
        assert(len(buf) > 0)

        if not g.fd:
            g.fd = os.open(os.path.join(g.opt.data, str(g.maxfile)),
                           os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                           0644)

        assert(g.size == os.fstat(g.fd).st_size)
        assert(len(buf) == os.write(g.fd, buf))
        os.fsync(g.fd)
        assert(g.offset+len(buf) == os.fstat(g.fd).st_size)

        size = g.size
        g.update(scan(g.opt.data, g.maxfile, g.offset, g.checksum,
                      kv_put, vclock_put))
        assert(g.size == size + len(buf))

        log('sent replication-request to({0}) file({1}) offset({2})'.format(
            src, g.maxfile, g.size))
        return dict(msg='replication_request', buf=g.json())
    except:
        traceback.print_exc()
        os._exit(0)


def on_message(src, msg, buf):
    return globals()[msg](src, buf)


def on_connect(src):
    return dict(msg='sync', buf=g.json())


def on_disconnect(src, exc, tb):
    if exc and str(exc) != 'reject-replication-request':
        log(tb)

    if src in g.peers:
        del(g.peers[src])

    if 'following-' + src == g.state:
        assert(not g.followers)

        g.state = ''
        if g.fd:
            os.fsync(g.fd)
            os.close(g.fd)
            g.fd = None
        log('')
        log('NO LEADER as {0} disconnected'.format(src))

    if src in g.followers:
        g.followers.pop(src)
        log('removed follower({0}) remaining({1})'.format(
            src, len(g.followers)))

    if g.state in ('old-sync', 'new-sync', 'leader'):
        if len(g.followers) < g.quorum:
            log('exiting as followers({0}) < quorum({1})'.format(
                len(g.followers), g.quorum))
            os._exit(0)


def on_stats(stats):
    g.stats = stats


def state(src, buf):
    return dict(buf=g.json())


def watch(src, buf):
    try:
        i = 0
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            txn = (struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0],
                   struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0])

            i += 24 + key_len

        return dict(buf=''.join([
            struct.pack('!B', 0),
            struct.pack('!Q', len('key')),
            'key',
            struct.pack('!Q', 1),
            struct.pack('!Q', 2),
            struct.pack('!Q', len('Hello')),
            'Hello']))
    except:
        return dict(buf=struct.pack('!B', 1) + traceback.format_exc())


def put(src, buf):
    if g.offset > g.opt.max:
        return

    try:
        i = 0
        buf_list = list()
        keys = list()
        size = 0
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            keys.append(key)
            filenum = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            offset = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
            value_len = struct.unpack('!Q', buf[i+24+key_len:i+32+key_len])[0]
            value = buf[i+32+key_len:i+32+key_len+value_len]

            f, t, v = g.kv.get(key, g.kv_tmp.get(key, (0, (0, 0), '')))
            assert((t[0] == filenum) and (t[1] == offset)), 'version mismatch'
            size += key_len + value_len

            buf_list.append(struct.pack('!Q', key_len))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', g.maxfile))
            buf_list.append(struct.pack('!Q', g.offset))
            buf_list.append(struct.pack('!Q', value_len))
            buf_list.append(value)

            i += 32 + key_len + value_len

        assert(i == len(buf)), 'invalid put request'

        updated_keys = list()
        while size > 0:
            if not g.key_list or g.key_list[0][1] == g.maxfile:
                break

            k, f, t = g.key_list.popleft()
            if k in g.kv and k not in keys and f != g.maxfile:
                if t == g.kv[k][1]:
                    buf_list.append(struct.pack('!Q', len(k)))
                    buf_list.append(k)
                    buf_list.append(struct.pack('!Q', t[0]))
                    buf_list.append(struct.pack('!Q', t[1]))
                    buf_list.append(struct.pack('!Q', len(g.kv[k][2])))
                    buf_list.append(g.kv[k][2])
                    updated_keys.append(k)
                    size -= len(k) + len(g.kv[k][2])

        res_list = [
            struct.pack('!B', 0),
            struct.pack('!Q', g.maxfile),
            struct.pack('!Q', g.offset)]

        append(''.join(buf_list))

        keys.extend(updated_keys)
        g.acks.append((g.offset, dict(dst=src, buf=''.join(res_list)), keys))
        return get_replication_responses()
    except:
        return dict(buf=struct.pack('!B', 1) + traceback.format_exc())


def append(buf):
    chksum = hashlib.sha1(g.checksum.decode('hex'))
    chksum.update(buf)

    if not g.fd:
        g.fd = os.open(os.path.join(g.opt.data, str(g.maxfile)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    os.write(g.fd, struct.pack('!Q', len(buf)) + buf + chksum.digest())

    g.update(scan(g.opt.data, g.maxfile, g.offset, g.checksum,
                  kv_tmp_put, vclock_put))


def get(src, buf):
    i = 0
    result = list()
    while i < len(buf):
        key_len = struct.unpack('!Q', buf[i:i+8])[0]
        key = buf[i+8:i+8+key_len]
        f, t, v = g.kv.get(key, (0, (0, 0), ''))

        result.append(struct.pack('!Q', key_len))
        result.append(key)
        result.append(struct.pack('!Q', t[0]))
        result.append(struct.pack('!Q', t[1]))
        result.append(struct.pack('!Q', len(v)))
        result.append(v)

        i += 8 + key_len

    return dict(buf=''.join(result))


def kv_put(d):
    key = d['key']
    if len(d['value']):
        g.kv[key] = (d['filenum'], d['txn'], d['value'])
        g.key_list.append((key, d['filenum'], d['txn']))
    elif key in g.kv:
        del(g.kv[key])


def kv_tmp_put(d):
    g.kv_tmp[d['key']] = (d['filenum'], d['txn'], d['value'])


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

                            txn = (
                                struct.unpack(
                                    '!Q', x[i+8+key_len:i+16+key_len])[0],
                                struct.unpack(
                                    '!Q', x[i+16+key_len:i+24+key_len])[0])

                            value_len = struct.unpack(
                                '!Q', x[i+24+key_len:i+32+key_len])[0]
                            value = x[i+32+key_len:i+32+key_len+value_len]

                            i += 32 + key_len + value_len

                            callback_kv(dict(key=key,
                                             filenum=filenum,
                                             offset=offset+i-value_len,
                                             txn=txn,
                                             value=value))
                    else:
                        callback_vclock(filenum, json.loads(x))

                    offset += len(x) + 28
                    assert(offset <= total_size)
                    assert(offset == fd.tell())

                    result.update(dict(
                        maxfile=filenum,
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
    g.quorum = int(len(g.opt.peers.split(','))/2.0 + 0.6)

    if not os.path.isdir(g.opt.data):
        os.mkdir(g.opt.data)

    try:
        with open(os.path.join(g.opt.data, 'reboot')) as fd:
            reboot = int(fd.read()) + 1
    except:
        reboot = 1
    finally:
        with open(os.path.join(g.opt.data, 'tmp'), 'w') as fd:
            fd.write(str(reboot))
        os.rename(os.path.join(g.opt.data, 'tmp'),
                  os.path.join(g.opt.data, 'reboot'))
        log('RESTARTING sequence({0})'.format(reboot))
        g.clock = (reboot, 0)

    files = map(int, filter(lambda x: x != 'reboot', os.listdir(g.opt.data)))
    if files:
        g.minfile = min(files)
        g.maxfile = min(files)
        try:
            with open(os.path.join(g.opt.data, str(g.minfile)), 'rb') as fd:
                vclklen = struct.unpack('!Q', fd.read(8))[0]
                g.vclocks[g.maxfile] = json.loads(fd.read(vclklen))
                g.checksum = fd.read(20).encode('hex')
                g.offset = fd.tell()
                fd.seek(0, 2)
                g.size = fd.tell()
                assert(g.offset == vclklen + 28)
        except:
            if g.minfile == g.maxfile:
                os.remove(os.path.join(g.opt.data, str(g.minfile)))

        g.__dict__.update(scan(g.opt.data, g.maxfile, g.offset, g.checksum,
                               kv_put, vclock_put))

        if g.kv:
            remove_max = min([g.kv[k][0] for k in g.kv])
        else:
            remove_max = g.maxfile

        for n in range(g.minfile, remove_max):
            os.remove(os.path.join(g.opt.data, str(n)))
            log('removed file({0})'.format(n))

        g.minfile = remove_max

        f = os.open(os.path.join(g.opt.data, str(g.maxfile)), os.O_RDWR)
        n = os.fstat(f).st_size
        if n > g.offset:
            os.ftruncate(f, g.offset)
            os.fsync(f)
            log('file({0}) truncated({1}) original({2})'.format(
                g.maxfile, g.offset, n))
            os.close(f)

        filenum = g.maxfile + 1
        while True:
            try:
                os.remove(os.path.join(g.opt.data, str(filenum)))
                log('removed file({0})'.format(filenum))
                filenum += 1
            except:
                break

    signal.alarm(random.randint(g.opt.timeout, 2*g.opt.timeout))

    return dict(port=g.opt.port,
                peers=g.opt.peers.split(','),
                cert=g.opt.cert)
