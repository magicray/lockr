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
import sqlite3
import logging
import hashlib
import optparse
import traceback
import collections

from logging import critical as log


class g:
    fd = None
    db = None
    kv = dict()
    src2key = dict()
    key2src = dict()
    header = dict()
    acks = collections.deque()
    peers = dict()
    followers = dict()
    quorum = 0
    clock = 0
    state = ''
    minfile = 0
    maxfile = 0
    offset = 0
    size = 0
    checksum = ''
    start_time = time.time()

    @classmethod
    def json(cls):
        t = time.time()
        x = time.strftime('%y%m%d.%H%M%S', time.gmtime(t))
        g.clock = x + '.' + str(int((t-int(t))*10**6))

        return json.dumps(dict(
            uptime=int(time.time()-g.start_time),
            state=g.state,
            node=os.getenv('MSGIO_NODE'),
            minfile=g.minfile,
            maxfile=g.maxfile,
            offset=g.offset,
            size=g.size,
            clock=g.clock,
            pending=[len(g.kv), len(g.acks)],
            watch=[len(g.src2key), len(g.key2src)],
            header=g.header,
            peers=dict([(k, dict(
                minfile=v['minfile'],
                maxfile=v['maxfile'],
                clock=v['clock'],
                offset=v['offset'],
                state=v['state'])) for k, v in g.peers.iteritems() if v])))


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
            log('WRITE enabled in msec(%03f)', (time.time()-g.start_time)*1000)

        return sync_broadcast_msg()

    if g.maxfile > 0 and g.peers[src]['maxfile'] == g.maxfile:
        peer_header = g.peers[src]['header']
        my_header = g.header

        for key in set(peer_header).intersection(set(my_header)):
            if peer_header[key] > my_header[key]:
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

        if (os.getenv('MSGIO_NODE') != leader[0]) and (count >= g.quorum):
            g.state = 'following-' + leader[0]
            log('LEADER(%s) selected due to %s', src, leader[2])

    if g.state.startswith('following-'):
        leader = g.state.split('following-')[1]
        log('sent replication-request to(%s) file(%d) offset(%d)',
            leader, g.maxfile, g.size)

        return sync_broadcast_msg() + [dict(dst=leader,
                                            msg='replication_request',
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

            header = json.dumps(vclk, sort_keys=True)

            append(header)
            os.fsync(g.fd)
            g.state = 'new-sync'
            log('new leader TERM({0}) header{1}'.format(g.maxfile, header))
            if 0 == int(time.time()) % 5:
                log('exiting to force test vclock conflict')
                os._exit(0)

            return sync_broadcast_msg() + get_replication_responses()

    acks = list()
    watch = list()
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

                offset, dst, keys = g.acks.popleft()
                for k in keys:
                    del(g.kv[k])
                    value = db_get(k)
                    for s in g.key2src.get(k, set()):
                        watch.append(dict(dst=s, buf=value))

                acks.append(dict(dst=dst, buf=''.join([
                    struct.pack('!B', 0),
                    struct.pack('!Q', g.maxfile),
                    struct.pack('!Q', offset)])))

    if acks:
        os.fsync(g.fd)
        g.db.commit()
    elif committed_offset == g.offset and g.offset > opt.max_size:
        log('max file size reached({0} > {1})'.format(g.offset, opt.max_size))
        os._exit(0)

    return acks + watch + get_replication_responses()


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

        hdr = g.db.execute('select header from file where file=?',
                           (req['maxfile'],)).fetchone()[0]
        reqhdr = json.dumps(req['header'], sort_keys=True)
        if req['size'] > 0 and hdr != reqhdr:
            log('header mismatch src(%s) file(%d)', src, req['maxfile'])
            log('local header %s', hdr)
            log('peer header %s', reqhdr)

            log('sent replication-truncate to(%s) file(%d) offset(%d) '
                'truncate(0)', src, req['maxfile'], req['size'])

            g.followers[src] = None
            msgs.append(dict(dst=src, msg='replication_truncate',
                             buf=json.dumps(dict(truncate=0))))

        with open(os.path.join(opt.data, str(req['maxfile']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['size']:
                log('sent replication-truncate to(%s) file(%d) offset(%d) '
                   'truncate(%d)', src, req['maxfile'], req['size'], fd.tell())

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_truncate',
                                 buf=json.dumps(dict(truncate=fd.tell()))))

            if fd.tell() == req['size']:
                if g.maxfile == req['maxfile']:
                    continue

                log('sent replication-nextfile to(%s) file(%d)',
                    src, req['maxfile']+1)

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                            buf=json.dumps(dict(filenum=req['maxfile']+1))))

            fd.seek(req['size'])
            buf = fd.read(opt.repl_size)
            if buf:
                log('sent replication-response to(%s) file(%d) offset(%d) '
                    'size(%d)',
                    src, req['maxfile'], req['offset'], len(buf))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_response', buf=buf))

    return msgs


def replication_truncate(src, buf):
    req = json.loads(buf)

    log('received replication-truncate from(%s) size(%d)',
        src, req['truncate'])

    f = os.open(os.path.join(opt.data, str(g.maxfile)), os.O_RDWR)
    n = os.fstat(f).st_size
    assert(req['truncate'] < n)
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

    return dict(msg='replication_request', buf=g.json())


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
        g.size = os.fstat(g.fd).st_size

        assert(g.checksum)
        scan(opt.data, g.maxfile, g.offset, g.checksum, db_put, header_put)
        g.db.commit()

        log('sent replication-request to(%s) file(%d) offset(%d)',
            src, g.maxfile, g.size)

        return [dict(msg='sync', buf=g.json()),
                dict(msg='replication_request', buf=g.json())]
    except:
        os._exit(0)


def on_message(src, msg, buf):
    return globals()[msg](src, buf)


def on_connect(src):
    return dict(msg='sync', buf=g.json())


def on_disconnect(src, exc, tb):
    if exc and str(exc) != 'reject-replication-request':
        log(tb)

    if src in g.src2key:
        key = g.src2key.pop(src)
        g.key2src[key].remove(src)
        if not g.key2src[key]:
            g.key2src.pop(key)

    if src not in g.peers:
        return

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
    g.src2key[src] = buf
    g.key2src.setdefault(buf, set()).add(src)

    return dict(buf=db_get(buf))


def put(src, buf):
    if g.offset > opt.max_size:
        return

    try:
        i = 0
        keys = dict()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]

            assert(key not in keys), 'duplicate key'
            assert(key not in g.kv), 'txn in progress'

            x = i + 8 + key_len
            old_len = struct.unpack('!Q', buf[x:x+8])[0]
            old = buf[x+8:x+8+old_len]

            x = i + 8 + key_len + 8 + old_len
            new_len = struct.unpack('!Q', buf[x:x+8])[0]
            new = buf[x+8:x+8+new_len]

            value = db_get(key)

            keys[key] = (new, value if old != value else None)

            i += 24 + key_len + old_len + new_len

        assert(i == len(buf)), 'invalid put request'

        if not all([keys[k][1] is None for k in keys]):
            buf_list = [struct.pack('!B', 1)]
            for k, v in keys.iteritems():
                if v[1] is not None:
                    buf_list.append(struct.pack('!Q', len(k)))
                    buf_list.append(k)
                    buf_list.append(struct.pack('!Q', len(v[1])))
                    buf_list.append(v[1])

            return dict(buf=''.join(buf_list))

        buf_list = list()
        for key in keys:
            buf_list.append(struct.pack('!Q', len(key)))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', len(keys[key][0])))
            buf_list.append(keys[key][0])

        g.kv.update(keys)

        rows = g.db.execute('''select key, file, length from data
                               where file < ?  order by file, offset limit ?
                            ''', (g.maxfile, len(keys))).fetchall()
        for r in filter(lambda r: (r[0] not in g.kv) and (r[2] > 0), rows):
            key = bytes(r[0])
            value = db_get(key)

            buf_list.append(struct.pack('!Q', len(key)))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', len(value)))
            buf_list.append(value)

        append(''.join(buf_list))

        g.acks.append((g.offset, src, set(keys)))
        return get_replication_responses()
    except:
        return dict(buf=struct.pack('!B', 2) + traceback.format_exc())


def append(buf):
    chksum = hashlib.sha1(g.checksum.decode('hex'))
    chksum.update(buf)

    if not g.fd:
        g.fd = os.open(os.path.join(opt.data, str(g.maxfile)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    os.write(g.fd, struct.pack('!Q', len(buf)) + buf + chksum.digest())
    g.size = os.fstat(g.fd).st_size

    scan(opt.data, g.maxfile, g.offset, g.checksum, db_put, header_put)


def keys(src, buf):
    rows = g.db.execute('select key from data where key like ? order by key',
                        (buf+'%',))
    result = list()
    for r in rows:
        key = bytes(r[0])
        result.append(struct.pack('!Q', len(key)))
        result.append(key)

    return dict(buf=''.join(result))


def get(src, buf):
    i = 0
    result = list()
    while i < len(buf):
        key_len = struct.unpack('!Q', buf[i:i+8])[0]
        key = buf[i+8:i+8+key_len]

        value = g.kv.get(key, db_get(key))

        result.append(struct.pack('!Q', len(value)))
        result.append(value)

        i += 8 + key_len

    return dict(buf=''.join(result))


def db_get(key):
    row = g.db.execute('select file, offset, length from data where key=?',
                       (key,)).fetchone()
    if row:
        filenum, offset, length = row
        with open(os.path.join(opt.data, str(filenum)), 'rb') as fd:
            fd.seek(offset, 0)
            return fd.read(length)

    return ''


def db_put(txn, filenum, offset, checksum):
    g.maxfile = filenum
    g.offset = offset
    g.checksum = checksum.encode('hex')

    for k, v in txn.iteritems():
        g.db.execute('delete from data where key=?', (k,))
        if v[1]:
            g.db.execute('insert into data values(?, ?, ?, ?)',
                         (k, filenum, v[0], len(v[1])))

    g.db.execute('''update file set length=?, checksum=? where file=?''',
                 (offset, g.checksum, filenum))


def header_put(header, filenum, offset, checksum):
    g.maxfile = filenum
    g.offset = offset
    g.checksum = checksum.encode('hex')
    g.header = json.loads(header)

    g.db.execute('''insert into file values(?, ?, ?, ?)''',
                 (filenum, header, offset, g.checksum))


def scan(path, filenum, offset, checksum, callback_kv, callback_vclock):
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

                    txn = dict()
                    if offset > 0:
                        i = 0
                        while i < len(x):
                            key_len = struct.unpack('!Q', x[i:i+8])[0]
                            key = x[i+8:i+8+key_len]

                            value_len = struct.unpack(
                                '!Q', x[i+8+key_len:i+16+key_len])[0]
                            value = x[i+16+key_len:i+16+key_len+value_len]

                            txn[key] = (offset+8+i+16+key_len, value)
                            i += 16 + key_len + value_len

                    offset += len(x) + 28
                    assert(offset <= total_size)
                    assert(offset == fd.tell())

                    if txn:
                        callback_kv(txn, filenum, offset, checksum)
                    else:
                        callback_vclock(x, filenum, offset, checksum)

                    log('scanned file(%d) offset(%d)', filenum, offset)

            filenum += 1
            offset = 0
        except:
            return


def init(peers):
    g.db = sqlite3.connect(opt.index)
    g.db.execute('''create table if not exists data(key blob primary key,
        file unsigned, offset unsigned, length unsigned)''')
    g.db.execute('create index if not exists data_file on data(file)')
    g.db.execute('''create table if not exists file(file unsigned primary key,
        header blob, length unsigned, checksum text)''')

    g.quorum = int(len(peers)/2.0 + 0.6)

    if not os.path.isdir(opt.data):
        os.mkdir(opt.data)

    files = map(int, os.listdir(opt.data))
    if not files:
        g.db.execute('delete from file')
        g.db.execute('delete from data')
        g.db.commit()
        return

    with open(os.path.join(opt.data, str(max(files))), 'rb') as fd:
        fd.seek(0, 2)
        size = fd.tell()

    if 0 == size:
        os.remove(os.path.join(opt.data, str(max(files))))
        log('removed file(%d)', max(files))
        os._exit(0)

    g.db.execute('delete from data where file < ?', (min(files),))
    g.db.execute('delete from data where file > ?', (max(files),))
    g.db.execute('delete from data where file = ? and offset+length > ?',
                 (max(files), size))

    g.db.execute('delete from file where file < ?', (min(files),))
    g.db.execute('delete from file where file > ?', (max(files),))
    g.db.execute('delete from file where file = ? and length > ?',
                 (max(files), size))
    g.db.commit()

    g.minfile = min(files)

    row = g.db.execute('''select file, header, length, checksum from file
                          order by file desc limit 1''').fetchone()
    if row:
        g.maxfile = row[0]
        g.header = json.loads(row[1])
        g.offset = row[2]
        g.checksum = row[3]
    else:
        g.maxfile = min(files)
        g.db.execute('delete from file')
        g.db.execute('delete from data')
        g.db.commit()

        try:
            with open(os.path.join(opt.data, str(g.minfile)), 'rb') as fd:
                hdrlen = struct.unpack('!Q', fd.read(8))[0]
                hdr = fd.read(hdrlen)
                g.header = json.loads(hdr)
                g.checksum = fd.read(20).encode('hex')
                g.offset = fd.tell()
                assert(g.offset == hdrlen + 28)

                g.db.execute('''insert into file values(?, ?, ?, ?)''',
                             (g.minfile, hdr, g.offset, g.checksum))
                g.db.commit()
        except:
            if g.minfile == g.maxfile:
                os.remove(os.path.join(opt.data, str(g.minfile)))
                log('removed file(%d)', g.minfile)
                os._exit(0)

    scan(opt.data, g.maxfile, g.offset, g.checksum, db_put, header_put)
    g.db.commit()

    with open(os.path.join(opt.data, str(g.maxfile)), 'rb') as fd:
        fd.seek(0, 2)
        g.size = fd.tell()

    quit = False
    if g.size > g.offset:
        f = os.open(os.path.join(opt.data, str(g.maxfile)), os.O_RDWR)
        os.ftruncate(f, g.offset)
        os.fsync(f)
        log('file(%d) truncated(%d) original(%d)', g.maxfile, g.offset, g.size)
        os.close(f)
        quit = True

    row = g.db.execute('select min(file) from data').fetchone()
    remove_max = row[0] if row[0] is not None else g.maxfile

    for n in range(g.minfile, remove_max):
        os.remove(os.path.join(opt.data, str(n)))
        log('removed file({0})'.format(n))
        quit = True

    g.minfile = remove_max

    remove_min = g.maxfile + 1
    while True:
        try:
            os.remove(os.path.join(opt.data, str(remove_min)))
            log('removed file({0})'.format(remove_min))
            quit = True
        except:
            break

    if quit:
        os._exit(0)


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
                            s.send('state')
                            stats = json.loads(s.recv())
                            log('connection to %s succeeded in %.03f msec' % (
                                srv, (time.time()-t)*1000))
                            if 'leader' == stats['state']:
                                self.server = s
                                log('connected to leader {0}'.format(srv))
                                break
                        except:
                            log('connection to %s failed in %.03f msec' % (
                                srv, (time.time()-t)*1000))

                self.server.send(req, buf)
                result = self.server.recv()
                log('received response(%s) from %s in %0.3f msec' % (
                    req, self.server.server, (time.time() - req_begin)*1000))
                return result
            except:
                self.server = None

        raise Exception('timed out')

    def state(self):
        return json.loads(self.request('state'))

    def watch(self, key):
        yield self.request('watch', key)
        while True:
            yield self.server.recv()

    def put(self, docs):
        items = list()
        for k, v in docs.iteritems():
            items.append(struct.pack('!Q', len(k)))
            items.append(k)
            items.append(struct.pack('!Q', len(v[0])))
            items.append(v[0])
            items.append(struct.pack('!Q', len(v[1])))
            items.append(v[1])

        buf = self.request('put', ''.join(items))
        if 0 == struct.unpack('!B', buf[0])[0]:
            filenum = struct.unpack('!Q', buf[1:9])[0]
            offset = struct.unpack('!Q', buf[9:17])[0]

            return 0, (filenum, offset)
        elif 1 == struct.unpack('!B', buf[0])[0]:
            kv = dict()
            i = 1
            while i < len(buf):
                key_len = struct.unpack('!Q', buf[i:i+8])[0]
                key = buf[i+8:i+8+key_len]
                value_len = struct.unpack('!Q',
                                          buf[i+8+key_len:i+16+key_len])[0]
                kv[key] = buf[i+16+key_len:i+16+key_len+value_len]

                i += 16 + key_len + value_len

            return 1, kv

        return struct.unpack('!B', buf[0])[0], buf[1:]

    def get(self, keys):
        buf = list()
        for key in keys:
            buf.append(struct.pack('!Q', len(key)))
            buf.append(key)

        buf = self.request('get', ''.join(buf))

        i = 0
        k = 0
        docs = dict()
        while i < len(buf):
            value_len = struct.unpack('!Q', buf[i:i+8])[0]
            value = buf[i+8:i+8+value_len]

            docs[keys[k]] = value
            k += 1

            i += 8 + value_len

        return docs

    def keys(self, prefix):
        buf = self.request('keys', prefix)

        i = 0
        result = list()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            result.append(buf[i+8:i+8+key_len])

            i += 8 + key_len

        return result


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

    def do_keys(self, line):
        prefix = shlex.split(line)[0] if line else ''
        for k in self.cli.keys(prefix):
            print(k)

    def do_get(self, line):
        result = self.cli.get(shlex.split(line))
        for k in sorted(result.keys()):
            print('{0} - {1}'.format(k, result[k]))

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
        docs = dict([(t[0], (t[1], t[2])) for t in tup])
        code, value = self.cli.put(docs)
        if 0 == code:
            print('committed : {0}'.format(value))
        elif 1 == code:
            print('compare failed for following keys:-')
            for k, v in value.iteritems():
                print('{0} - {1}'.format(k, v))
        else:
            print(value)

    def do_watch(self, line):
        while True:
            try:
                for value in self.cli.watch(shlex.split(line)[0]):
                    print(value)
            except:
                pass


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='int',
                      help='port number')
    parser.add_option('--nodes', dest='nodes', type='string',
                      help='comma separated list of server:port')
    parser.add_option('--data', dest='data', type='string',
                      help='data directory', default='data')
    parser.add_option('--index', dest='index', type='string',
                      help='index file path', default='snapshot.db')
    parser.add_option('--maxsize', dest='max_size', type='int',
                      help='max file size', default='256')
    parser.add_option('--replsize', dest='repl_size', type='int',
                      help='replication chunk size', default=10*2**20)
    parser.add_option('--timeout', dest='timeout', type='int',
                      help='timeout in seconds', default='20')

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
