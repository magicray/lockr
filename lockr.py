import os
import sys
import cmd
import time
import json
import shlex
import msgio
import struct
import signal
import random
import pprint
import logging
import hashlib
import optparse
import traceback
import collections

from logging import critical as log


class g:
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
        g.clock = (g.clock[0], g.clock[1]+1)

        return json.dumps(dict(
            state=g.state,
            node=os.getenv('MSGIO_NODE'),
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


def sync_broadcast_msg():
    return [dict(dst=p, msg='sync', buf=g.json()) for p in g.peers]


def sync(src, buf):
    g.peers[src] = json.loads(buf)

    if g.peers[src]['state'].startswith('following-'):
        if g.state.startswith('following-'):
            if g.state < g.peers[src]['state']:
                log('exiting as following a suboptimal leader(%s)', g.state)
                os._exit(0)

    if g.state.startswith('following-') or g.state in ('old-sync', 'leader'):
        return

    if 'new-sync' == g.state:
        in_sync = filter(lambda k: g.maxfile == g.peers[k]['maxfile'],
                         filter(lambda k: g.offset == g.peers[k]['offset'],
                                g.peers))

        if len(in_sync) >= g.quorum:
            log('quorum(%d >= %d) in sync with new session',
                len(in_sync), g.quorum)

            g.state = 'leader'
            log('WRITE enabled')

            return sync_broadcast_msg()

    if g.peers[src]['maxfile'] == g.maxfile:
        peer_vclock = g.peers[src]['vclock']
        my_vclock = g.vclocks.get(g.maxfile, dict())

        for key in set(peer_vclock).intersection(set(my_vclock)):
            if peer_vclock[key] > my_vclock[key]:
                os.remove(os.path.join(opt.data, str(g.maxfile)))
                log('REMOVED file(%d) as vclock(%s) is ahead', g.maxfile, src)
                os._exit(0)

    if g.maxfile > 0 and g.peers[src]['minfile'] > g.maxfile:
        log('src(%s) minfile(%d) > maxfile(%d)',
            src, g.peers[src]['minfile'], g.maxfile)
        while True:
            try:
                os.remove(os.path.join(opt.data, str(g.maxfile)))
                log('removed file(%d)', g.maxfile)
                g.maxfile -= 1
            except:
                os._exit(0)
                break

    if g.peers[src]['state'] in ('old-sync', 'new-sync', 'leader'):
        g.state = 'following-' + src
        log('LEADER(%s) identified', src)
    else:
        leader = (os.getenv('MSGIO_NODE'), g.__dict__, 'self')
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

            if k > leader[0]:
                leader = (k, p, '{0} > {1}'.format(k, leader[0]))

        if (src == leader[0]) and (count >= g.quorum):
            g.state = 'following-' + src
            log('LEADER(%s) selected due to %s', src, leader[2])

    if g.state.startswith('following-'):
        log('sent replication-request to(%s) file(%d) offset(%d)',
            src, g.maxfile, g.size)

        return sync_broadcast_msg() + [dict(msg='replication_request',
                                            buf=g.json())]


def replication_request(src, buf):
    req = json.loads(buf)

    log('received replication-request from(%s) file(%d) offset(%d)',
        src, req['maxfile'], req['offset'])

    if g.state.startswith('following-'):
        log('rejecting replication-request from(%s) as (%s)', src, g.state)
        raise Exception('reject-replication-request')

    if src not in g.followers:
        log('accepted (%s) as follower(%d)', src, len(g.followers)+1)

    g.followers[src] = req

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

            vclk = {os.getenv('MSGIO_NODE'): g.clock}
            for k in filter(lambda k: g.peers[k], g.peers):
                vclk[k] = g.peers[k]['clock']

            vclk = json.dumps(vclk)

            append(vclk)
            os.fsync(g.fd)
            g.state = 'new-sync'
            log('new leader SESSION({0}) VCLK{1}'.format(g.maxfile, vclk))
            if int(time.time()) % 2:
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
    elif committed_offset == g.offset and g.offset > opt.max_size:
        log('max file size reached({0} > {1})'.format(g.offset, opt.max_size))
        os._exit(0)

    return sync_broadcast_msg() + acks + get_replication_responses()


def get_replication_responses():
    msgs = list()
    for src in filter(lambda k: g.followers[k], g.followers.keys()):
        req = g.followers[src]

        if 0 == req['maxfile']:
            f = map(int, filter(lambda x: x != 'reboot',
                                os.listdir(opt.data)))
            if f:
                log('sent replication-nextfile to(%s) file(%d)', src, min(f))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(opt.data, str(req['maxfile']))):
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

        with open(os.path.join(opt.data, str(req['maxfile']))) as fd:
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
                log('sent replication-response to(%s) file(%d) offset(%d) '
                    'size(%d)',
                    src, req['maxfile'], req['offset'], len(buf))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_response', buf=buf))

    return msgs


def replication_truncate(src, buf):
    req = json.loads(buf)

    log('received replication-truncate from{0} size({1})'.format(
        src, req['truncate']))

    f = os.open(os.path.join(opt.data, str(g.maxfile)), os.O_RDWR)
    n = os.fstat(f).st_size
    os.ftruncate(f, req['truncate'])
    os.fsync(f)
    os.close(f)

    log('file({0}) truncated({1}) original({2})'.format(
        g.maxfile, req['truncate'], n))
    os._exit(0)


def replication_nextfile(src, buf):
    req = json.loads(buf)

    log('received replication-nextfile from(%s) filenum(%d)',
        src, req['filenum'])

    g.maxfile = req['filenum']
    g.offset = 0
    g.size = 0

    if g.fd:
        os.fsync(g.fd)
        os.close(g.fd)
        g.fd = None

    log('sent replication-request to(%s) file(%d) offset(%d)',
        src, g.maxfile, g.size)
    return sync_broadcast_msg() + [dict(msg='replication_request',
                                        buf=g.json())]


def replication_response(src, buf):
    log(('received replication-response from({0}) size({1})').format(
        src, len(buf)))

    try:
        assert(src == g.state.split('-')[1])
        assert(len(buf) > 0)

        if not g.fd:
            g.fd = os.open(os.path.join(opt.data, str(g.maxfile)),
                           os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                           0644)

        assert(g.size == os.fstat(g.fd).st_size)
        assert(len(buf) == os.write(g.fd, buf))
        os.fsync(g.fd)
        assert(g.offset+len(buf) == os.fstat(g.fd).st_size)

        size = g.size
        g.update(scan(opt.data, g.maxfile, g.offset, g.checksum,
                      kv_put, vclock_put))
        assert(g.size == size + len(buf))

        log('sent replication-request to(%s) file(%d) offset(%d)',
            src, g.maxfile, g.size)
        return sync_broadcast_msg() + [dict(msg='replication_request',
                                            buf=g.json())]
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
        log('exiting as LEADER({0}) disconnected'.format(src))
        os._exit(0)

    if src in g.followers:
        g.followers.pop(src)
        log('removed follower(%s) remaining(%d)', src, len(g.followers))

    if g.state in ('old-sync', 'new-sync', 'leader'):
        if len(g.followers) < g.quorum:
            log('exiting as followers(%d) < quorum(%d)',
                len(g.followers), g.quorum)
            os._exit(0)


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
    if g.offset > opt.max_size:
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
        g.fd = os.open(os.path.join(opt.data, str(g.maxfile)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    os.write(g.fd, struct.pack('!Q', len(buf)) + buf + chksum.digest())

    g.update(scan(opt.data, g.maxfile, g.offset, g.checksum,
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


def init(peers):
    g.quorum = int(len(peers)/2.0 + 0.6)

    if not os.path.isdir(opt.data):
        os.mkdir(opt.data)

    try:
        with open(os.path.join(opt.data, 'reboot')) as fd:
            reboot = int(fd.read()) + 1
    except:
        reboot = 1
    finally:
        with open(os.path.join(opt.data, 'tmp'), 'w') as fd:
            fd.write(str(reboot))
        os.rename(os.path.join(opt.data, 'tmp'),
                  os.path.join(opt.data, 'reboot'))
        log('RESTARTING sequence({0})'.format(reboot))
        g.clock = (reboot, 0)

    files = map(int, filter(lambda x: x != 'reboot', os.listdir(opt.data)))
    if files:
        g.minfile = min(files)
        g.maxfile = min(files)
        try:
            with open(os.path.join(opt.data, str(g.minfile)), 'rb') as fd:
                vclklen = struct.unpack('!Q', fd.read(8))[0]
                g.vclocks[g.maxfile] = json.loads(fd.read(vclklen))
                g.checksum = fd.read(20).encode('hex')
                g.offset = fd.tell()
                fd.seek(0, 2)
                g.size = fd.tell()
                assert(g.offset == vclklen + 28)
        except:
            if g.minfile == g.maxfile:
                os.remove(os.path.join(opt.data, str(g.minfile)))

        g.__dict__.update(scan(opt.data, g.maxfile, g.offset, g.checksum,
                               kv_put, vclock_put))

        if g.kv:
            remove_max = min([g.kv[k][0] for k in g.kv])
        else:
            remove_max = g.maxfile

        for n in range(g.minfile, remove_max):
            os.remove(os.path.join(opt.data, str(n)))
            log('removed file({0})'.format(n))

        g.minfile = remove_max

        f = os.open(os.path.join(opt.data, str(g.maxfile)), os.O_RDWR)
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
                os.remove(os.path.join(opt.data, str(filenum)))
                log('removed file({0})'.format(filenum))
                filenum += 1
            except:
                break


class Lockr(object):
    def __init__(self, servers, timeout=30):
        self.servers = servers
        self.server = None
        self.timeout = timeout

    def request(self, req, buf=''):
        req_begin = time.time()
        while time.time() < (req_begin + self.timeout):
            try:
                if not self.server:
                    for srv in self.servers:
                        try:
                            t = time.time()
                            s = msgio.Client(srv)
                            stats = json.loads(s.request('state'))
                            log('connection to %s succeeded in %.03f msec' % (
                                srv, (time.time()-t)*1000))
                            if 'leader' == stats['state']:
                                self.server = s
                                log('connected to leader {0}'.format(srv))
                                break
                        except:
                            log('connection to %s failed in %.03f msec' % (
                                srv, (time.time()-t)*1000))

                result = self.server.request(req, buf)
                log('received response(%s) from %s in %0.3f msec' % (
                    req, self.server.server, (time.time() - req_begin)*1000))
                return result
            except:
                self.server = None

        raise Exception('timed out')

    def state(self):
        return json.loads(self.request('state'))

    def watch(self, docs):
        buf = list()
        for k, v in docs.iteritems():
            buf.append(struct.pack('!Q', len(k)))
            buf.append(k)
            buf.append(struct.pack('!Q', v[0]))
            buf.append(struct.pack('!Q', v[1]))

        buf = self.request('watch', ''.join(buf))

        if 0 == struct.unpack('!B', buf[0])[0]:
            key_len = struct.unpack('!Q', buf[1:9])[0]
            key = buf[9:9+key_len]
            txn = (struct.unpack('!Q', buf[9+key_len:17+key_len])[0],
                   struct.unpack('!Q', buf[17+key_len:25+key_len])[0])
            value_len = struct.unpack('!Q', buf[25+key_len:33+key_len])[0]
            value = buf[33+key_len:33+key_len+value_len]

            return (0, (key, txn, value))
        else:
            return struct.unpack('!B', buf[0])[0], buf[1:]

    def put(self, docs):
        items = list()
        for k, v in docs.iteritems():
            ver = '0-0' if (v[0] is '-' or v[0] is None) else v[0]

            items.append(struct.pack('!Q', len(k)))
            items.append(k)
            items.append(struct.pack('!Q', int(ver.split('-')[0])))
            items.append(struct.pack('!Q', int(ver.split('-')[1])))
            items.append(struct.pack('!Q', len(v[1])))
            items.append(v[1])

        buf = self.request('put', ''.join(items))
        if 0 == struct.unpack('!B', buf[0])[0]:
            f = struct.unpack('!Q', buf[1:9])[0]
            o = struct.unpack('!Q', buf[9:17])[0]

            return 0, (f, o)

        return struct.unpack('!B', buf[0])[0], buf[1:]

    def get(self, keys):
        buf = list()
        for key in keys:
            buf.append(struct.pack('!Q', len(key)))
            buf.append(key)

        buf = self.request('get', ''.join(buf))

        i = 0
        docs = dict()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            f = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            o = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
            value_len = struct.unpack('!Q', buf[i+24+key_len:i+32+key_len])[0]
            value = buf[i+32+key_len:i+32+key_len+value_len]

            docs[key] = ('{0}-{1}'.format(f, o), value)

            i += 32 + key_len + value_len

        return docs


class Client(cmd.Cmd):
    prompt = '>'

    def __init__(self, servers):
        cmd.Cmd.__init__(self)
        self.cli = Lockr(servers)

    def do_EOF(self, line):
        self.do_quit(line)

    def do_quit(self, line):
        exit(0)

    def do_state(self, line):
        print(pprint.pformat(self.cli.state()).replace("u'", " '"))

    def do_get(self, line):
        for k, v in self.cli.get(line.split()).iteritems():
            print('{0} <{1}> {2}'.format(k, v[0], v[1]))

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
        docs = dict([(t[0], (t[1].strip('<>'), t[2])) for t in tup])
        code, value = self.cli.put(docs)
        if 0 == code:
            print('<{0}-{1}>'.format(value[0], value[1]))
        else:
            print('{0}'.format(value))

    def do_watch(self, line):
        cmd = shlex.split(line)

        code, result = self.cli.watch(
            dict(map(lambda x: (x[0], tuple(map(int,
                                                x[1].strip('<>').split('-')))),
                     zip(cmd[0::2], cmd[1::2]))))
        if 0 == code:
            key, txn, value = result
            print('{0} <{1}-{2}> {3}'.format(key, txn[0], txn[1], value))
        else:
            print(result)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='int',
                      help='port number')
    parser.add_option('--nodes', dest='nodes', type='string',
                      help='comma separated list of server:port')
    parser.add_option('--data', dest='data', type='string',
                      help='data directory', default='data')
    parser.add_option('--maxsize', dest='max_size', type='int',
                      help='max file size', default='256')
    parser.add_option('--timeout', dest='timeout', type='int',
                      help='timeout in seconds', default='30')

    opt, args = parser.parse_args()

    logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

    nodes = set(map(lambda x: (x.split(':')[0], int(x.split(':')[1])),
                    opt.nodes.split(',')))

    if opt.port:
        init(nodes)
        signal.alarm(random.randint(opt.timeout, 2*opt.timeout))
        msgio.loop(sys.modules[__name__], opt.port, nodes)
    else:
        Client(nodes).cmdloop()
