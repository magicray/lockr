import os
import json
import time
import fcntl
import struct
import sqlite3
import hashlib
import traceback
import collections

from logging import critical as log


class g:
    opt = None
    dfd = None
    fd = None
    db = None
    kv = set()
    watch = dict()
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
    chksum = None
    checksum = ''
    node = None
    start_time = time.time()

    @classmethod
    def json(cls):
        t = time.time()
        x = time.strftime('%y%m%d.%H%M%S', time.gmtime(t))
        g.clock = x + '.' + str(int((t-int(t))*10**6))

        return json.dumps(dict(
            uptime=int(time.time()-g.start_time),
            state=g.state,
            node=g.node,
            minfile=g.minfile,
            maxfile=g.maxfile,
            offset=g.offset,
            size=g.size,
            clock=g.clock,
            header=json.loads(get_header(g.maxfile)),
            pending=[len(g.kv), len(g.acks), len(g.watch)],
            peers=dict([(k, dict(
                minfile=v['minfile'],
                maxfile=v['maxfile'],
                clock=v['clock'],
                offset=v['offset'],
                state=v['state'])) for k, v in g.peers.iteritems() if v])))


def vote(src, buf):
    g.peers[src] = json.loads(buf)

    if g.state:
        return

    if g.maxfile > 0 and g.peers[src]['maxfile'] == g.maxfile:
        peer_header = g.peers[src]['header']
        my_header = json.loads(get_header(g.maxfile))

        for key in set(peer_header).intersection(set(my_header)):
            if peer_header[key] > my_header[key]:
                os.remove(os.path.join(g.opt.data, str(g.maxfile)))
                log('REMOVED file(%d) as vclock(%s) is ahead', g.maxfile, src)
                os._exit(0)

    if g.maxfile > 0 and g.peers[src]['minfile'] > g.maxfile:
        log('src(%s) minfile(%d) > maxfile(%d)',
            src, g.peers[src]['minfile'], g.maxfile)
        while True:
            try:
                os.remove(os.path.join(g.opt.data, str(g.maxfile)))
                log('removed file(%d)', g.maxfile)
                g.maxfile -= 1
            except:
                os._exit(0)

    if g.peers[src]['state'] in ('old-sync', 'new-sync', 'leader'):
        g.state = 'following-' + src
        reason = 'leader identified'
    else:
        leader = (g.node, g.__dict__, 'self')
        count = 0
        for k, v in g.peers.iteritems():
            if v['state'].startswith('following-'):
                continue

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

        if (g.node != leader[0]) and (count >= g.quorum):
            g.state = 'following-' + leader[0]
            reason = leader[2]

    if g.state.startswith('following-'):
        leader = g.state.split('following-')[1]

        log('sent replication-request to(%s) file(%d) offset(%d) as %s',
            leader, g.maxfile, g.size, reason)
        msgs = [dict(dst=leader, msg='replication_request', buf=g.json())]

        for f in g.followers:
            log('sent leader-conflict to(%s) leader(%s)', f, leader)
            msgs.append(dict(dst=f, msg='leader_conflict'))

        return msgs


def leader_conflict(src, buf):
    log('received leader_conflict from(%s)', src)
    os._exit(0)


def replication_request(src, buf):
    req = json.loads(buf)
    g.peers[src] = req

    log('received replication-request from(%s) file(%d) offset(%d)',
        src, req['maxfile'], req['offset'])

    if g.state.startswith('following-'):
        log('rejecting replication-request from(%s)', src)
        raise Exception('reject-replication-request')

    if src not in g.followers:
        log('accepted (%s) as follower(%d)', src, len(g.followers)+1)

    g.followers[src] = req

    if not g.state and len(g.followers) == g.quorum:
        g.state = 'old-sync'
        log('assuming LEADERSHIP as quorum reached({0})'.format(g.quorum))

    if 'new-sync' == g.state:
        in_sync = filter(lambda k: g.maxfile == g.peers[k]['maxfile'],
                         filter(lambda k: g.offset == g.peers[k]['offset'],
                                g.peers))

        if len(in_sync) >= g.quorum:
            g.state = 'leader'
            log('WRITE enabled in msec(%03f) quorum(%d >= %d)',
                (time.time()-g.start_time)*1000, len(in_sync), g.quorum)

            msgs = list()
            for f in set(g.peers) - set(g.followers):
                log('sent leader-conflict to non-follower(%s)', f)
                msgs.append(dict(dst=f, msg='leader_conflict'))
            return msgs + get_replication_responses()

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

            vclk = {g.node: g.clock}
            for k in filter(lambda k: g.peers[k], g.peers):
                vclk[k] = g.peers[k]['clock']

            header = json.dumps(vclk, sort_keys=True)

            append(header)
            commit()
            g.state = 'new-sync'
            log('new leader TERM({0}) header{1}'.format(g.maxfile, header))
            if 0 == int(time.time()) % 5:
                log('exiting to force test vclock conflict')
                os._exit(0)

            return get_replication_responses()

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

                offset, dst, keys = g.acks.popleft()
                for k in keys:
                    g.kv.remove(k)

                acks.append(dict(dst=dst, buf=''.join([
                    struct.pack('!B', 0),
                    struct.pack('!Q', g.maxfile),
                    struct.pack('!Q', offset)])))

    if acks:
        scan(g.maxfile, g.offset, g.checksum, g.maxfile, committed_offset)
        commit()
        for k in g.watch.keys():
            f, o = g.watch[k]
            if g.maxfile > f or (g.maxfile == f and g.offset > o):
                g.watch.pop(k)
                acks.append(dict(dst=k))
    elif committed_offset == g.offset and g.offset > g.opt.max_size:
        log('max file size reached(%d > %d)', g.offset, g.opt.max_size)
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
                log('sent replication-nextfile to(%s) file(%d)', src, min(f))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(g.opt.data, str(req['maxfile']))):
            continue

        hdr = get_header(req['maxfile'])
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

        with open(os.path.join(g.opt.data, str(req['maxfile']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['size']:
                log('sent replication-truncate to(%s) file(%d) offset(%d) '
                    'truncate(%d)',
                    src, req['maxfile'], req['size'], fd.tell())

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
            buf = fd.read(g.opt.repl_size)
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

    f = os.open(os.path.join(g.opt.data, str(g.maxfile)), os.O_RDWR)
    n = os.fstat(f).st_size
    assert(req['truncate'] < n)
    os.ftruncate(f, req['truncate'])

    log('file(%d) truncated(%d) original(%d)', g.maxfile, req['truncate'], n)
    os._exit(0)


def replication_nextfile(src, buf):
    req = json.loads(buf)

    log('received replication-nextfile from(%s) filenum(%d)',
        src, req['filenum'])

    g.maxfile = req['filenum']
    g.offset = 0
    g.size = 0

    if g.fd:
        commit()
        g.fd = None

    log('sent replication-request to(%s) file(%d) offset(%d)',
        src, g.maxfile, g.size)

    return dict(msg='replication_request', buf=g.json())


def replication_response(src, buf):
    log('received replication-response from(%s) size(%d)', src, len(buf))

    try:
        assert(src == g.state.split('-')[1])
        assert(len(buf) > 0)

        if not g.fd:
            g.fd = os.open(os.path.join(g.opt.data, str(g.maxfile)),
                           os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                           0644)

        assert(g.size == os.fstat(g.fd).st_size)
        assert(len(buf) == os.write(g.fd, buf))

        g.size = os.fstat(g.fd).st_size

        scan(g.maxfile, g.offset, g.checksum)
        commit()

        log('sent replication-request to(%s) file(%d) offset(%d)',
            src, g.maxfile, g.size)

        return dict(msg='replication_request', buf=g.json())
    except:
        os._exit(0)


def on_message(src, msg, buf):
    if type(src) is tuple:
        try:
            assert(msg in ('get', 'put', 'watch', 'state')), 'invalid command'
            return globals()[msg](src, buf)
        except:
            return dict(buf=struct.pack('!B', 255) + traceback.format_exc())

    return globals()[msg](src, buf)


def on_listen(node):
    g.node = node


def on_connect(src):
    return dict(msg='vote', buf=g.json())


def on_disconnect(src, exc, tb):
    if exc and str(exc) != 'reject-replication-request':
        log(tb)

    if src not in g.peers:
        g.watch.pop(src, None)
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
    filenum = struct.unpack('!Q', buf[0:8])[0]
    offset = struct.unpack('!Q', buf[8:16])[0]

    if (g.maxfile > filenum) or (g.maxfile == filenum and g.offset > offset):
        return dict()

    g.watch[src] = (filenum, offset)


def get(src, buf):
    filenum = struct.unpack('!Q', buf[0:8])[0]
    offset = struct.unpack('!Q', buf[8:16])[0]
    flags = struct.unpack('!I', buf[16:20])[0]

    keys = list()
    i = 20
    while i < len(buf):
        begin_len = struct.unpack('!Q', buf[i:i+8])[0]
        begin = buf[i+8:i+8+begin_len]
        end_len = struct.unpack('!Q', buf[i+8+begin_len:i+16+begin_len])[0]
        end = buf[i+16+begin_len:i+16+begin_len+end_len]

        if not begin:
            begin = g.db.execute('select min(key) from data').fetchone()[0]
        else:
            begin = sqlite3.Binary(begin)

        if not end:
            end = g.db.execute('select max(key) from data').fetchone()[0]
        else:
            end = sqlite3.Binary(end)

        keys.append((begin, end))

        i += 16 + begin_len + end_len

    assert(i == len(buf))

    result = [struct.pack('!B', 0),
              struct.pack('!Q', g.maxfile),
              struct.pack('!Q', g.offset)]

    for begin_key, end_key in keys:
        full = g.db.execute('''select key, length from data
                               where key between ? and ?''',
            (begin_key, end_key)).fetchall()

        if 0 == filenum and 0 == offset:
            new = full
        else:
            new = g.db.execute('''select key, length from data
                                  where key between ? and ?
                                  and ((file = ? and offset > ?) or
                                       (file > ?))
                                  and gc = 0''',
                (begin_key, end_key, filenum, offset, filenum)).fetchall()

        new = dict([(bytes(r[0]), r[1]) for r in new if '' != r[0]])

        for key, length in new.iteritems():
            result.append(struct.pack('!Q', len(key)))
            result.append(key)
            result.append(struct.pack('!Q', length))
            if flags:
                result.append(db_get(key))

        for k, length in full:
            key = bytes(k)
            if key in new or '' == key:
                continue

            result.append(struct.pack('!Q', len(key)))
            result.append(key)
            result.append(struct.pack('!Q', 0))

    return dict(buf=''.join(result))


def put(src, buf):
    if g.offset > g.opt.max_size:
        return

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
    assert(len(keys)), 'empty request'

    if not all([keys[k][1] is None for k in keys]):
        buf_list = [struct.pack('!B', 1)]
        for k, v in keys.iteritems():
            if v[1] is not None:
                buf_list.append(struct.pack('!Q', len(k)))
                buf_list.append(k)
                buf_list.append(struct.pack('!Q', len(v[1])))
                buf_list.append(v[1])

        return dict(buf=''.join(buf_list))

    g.kv.update(keys)

    buf_list = list()
    for key in keys:
        buf_list.append(struct.pack('!Q', len(key)))
        buf_list.append(key)
        buf_list.append(struct.pack('!Q', len(keys[key][0])))
        buf_list.append(keys[key][0])

    append(''.join(buf_list))

    rows = g.db.execute('''select key from data
                           where file < ? and length > 0
                           order by file, offset limit ?
                        ''', (g.maxfile, len(keys)*2)).fetchall()

    buf_list = list()
    for r in filter(lambda r: bytes(r[0]) not in g.kv, rows)[0:len(keys)]:
        key = bytes(r[0])
        value = db_get(key)

        buf_list.append(struct.pack('!Q', len(key)))
        buf_list.append(key)
        buf_list.append(struct.pack('!Q', len(value)))
        buf_list.append(value)

    append(''.join(buf_list), True)

    g.acks.append((g.size, src, set(keys)))
    return get_replication_responses()


def append(buf, gc=False):
    if not g.chksum:
        g.chksum = hashlib.sha1(g.checksum.decode('hex'))
    else:
        g.chksum = hashlib.sha1(g.chksum.digest())

    gc = struct.pack('!I', gc)

    g.chksum.update(gc)
    g.chksum.update(buf)

    if not g.fd:
        g.fd = os.open(os.path.join(g.opt.data, str(g.maxfile)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    length = struct.pack('!Q', len(buf) + 4)
    os.write(g.fd, length + gc + buf + g.chksum.digest())
    g.size = os.fstat(g.fd).st_size


def db_get(key):
    row = g.db.execute('select file, offset, length from data where key=?',
                       (sqlite3.Binary(key),)).fetchone()
    if row:
        filenum, offset, length = row
        with open(os.path.join(g.opt.data, str(filenum)), 'rb') as fd:
            fd.seek(offset, 0)
            buf = fd.read(length)

            assert(len(buf) == length)
            return buf

    return ''


def db_put(txn, filenum, offset, checksum, flags):
    g.maxfile = filenum
    g.offset = offset
    g.checksum = checksum.encode('hex')

    for k, v in txn.iteritems():
        g.db.execute('delete from data where key=?', (sqlite3.Binary(k),))
        if v[1]:
            g.db.execute('insert into data values(?, ?, ?, ?, ?)',
                         (sqlite3.Binary(k), filenum, v[0], len(v[1]), flags))


def header_put(header, filenum, offset, checksum):
    g.maxfile = filenum
    g.offset = offset
    g.checksum = checksum.encode('hex')

    g.db.execute('delete from data where key is null')
    g.db.execute('insert into data values(null, ?, ?, ?, ?)',
                 (filenum, 8, len(header)+4, False))


def scan(filenum, offset, checksum, e_filenum=2**64-1, e_offset=2**64-1,
         cb_txn=db_put, cb_hdr=header_put):

    checksum = checksum.decode('hex')
    while True:
        try:
            assert(filenum <= e_filenum)

            with open(os.path.join(g.opt.data, str(filenum)), 'rb') as fd:
                fd.seek(0, 2)
                total_size = fd.tell()
                fd.seek(offset)

                while(offset < total_size):
                    l = struct.unpack('!Q', fd.read(8))[0]
                    assert(l + 28 <= e_offset)

                    x = fd.read(l)
                    y = fd.read(20)
                    flags = struct.unpack('!I', x[0:4])[0]

                    chksum = hashlib.sha1(checksum)
                    chksum.update(x)
                    assert(y == chksum.digest())
                    checksum = y

                    txn = dict()
                    if offset > 0:
                        i = 4
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
                        cb_txn(txn, filenum, offset, checksum, flags)
                    else:
                        cb_hdr(x[4:], filenum, offset, checksum)

                    log('scanned file(%d) offset(%d)', filenum, offset)

            filenum += 1
            offset = 0
        except:
            return


def get_header(filenum):
    if not os.path.isfile(os.path.join(g.opt.data, str(filenum))):
        return json.dumps(dict())

    with open(os.path.join(g.opt.data, str(filenum)), 'rb') as fd:
        hdrlen = struct.unpack('!Q', fd.read(8))[0]
        return fd.read(hdrlen)[4:]


def commit():
    if g.fd:
        os.fsync(g.fd)
    os.fsync(g.dfd)
    g.db.commit()


def init(peers, opt):
    g.opt = opt
    g.db = sqlite3.connect(g.opt.index)
    g.db.execute('''create table if not exists data(key blob primary key,
        file unsigned, offset unsigned, length unsigned, gc unsigned)''')
    g.db.execute('create index if not exists data_1 on data(file, offset)')
    g.db.execute('create index if not exists data_2 on data(length)')

    g.quorum = int(len(peers)/2.0 + 0.6)

    if not os.path.isdir(g.opt.data):
        os.mkdir(g.opt.data)

    g.dfd = os.open(g.opt.data, os.O_RDONLY)
    fcntl.flock(g.dfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    files = map(int, os.listdir(g.opt.data))
    if not files:
        g.db.execute('delete from data')
        commit()
        return

    try:
        g.db.execute('delete from data where file < ?', (min(files),))
        g.minfile = min(files)

        row = g.db.execute('''select file, offset, length from data
                              order by file desc, offset desc limit 1
                           ''').fetchone()
        if row:
            g.maxfile = row[0]
            g.offset = row[1] + row[2] + 20
            with open(os.path.join(g.opt.data, str(g.maxfile)), 'rb') as fd:
                fd.seek(row[1] + row[2])
                g.checksum = fd.read(20).encode('hex')
                assert(40 == len(g.checksum))
        else:
            g.maxfile = min(files)
            g.db.execute('delete from data')

            with open(os.path.join(g.opt.data, str(g.minfile)), 'rb') as fd:
                hdrlen = struct.unpack('!Q', fd.read(8))[0]
                hdr = fd.read(hdrlen)
                g.checksum = fd.read(20).encode('hex')
                g.offset = fd.tell()
                assert(g.offset == hdrlen + 28)

        scan(g.maxfile, g.offset, g.checksum)

        g.size = g.offset

        row = g.db.execute('select min(file) from data').fetchone()
        g.minfile = row[0] if row[0] is not None else g.maxfile

        for n in range(min(files),g.minfile) + range(g.maxfile+1,max(files)+1):
            os.remove(os.path.join(g.opt.data, str(n)))
            log('removed file({0})'.format(n))

        f = os.open(os.path.join(g.opt.data, str(g.maxfile)), os.O_RDWR)
        n = os.fstat(f).st_size
        if n > g.offset:
            os.ftruncate(f, g.offset)
            os.fsync(f)
            log('file(%d) truncated(%d) original(%d)', g.maxfile, g.offset, n)
        os.close(f)

        commit()
    except:
        g.db.execute('delete from data where file >= ?', (g.maxfile,))
        commit()
        os._exit(0)
