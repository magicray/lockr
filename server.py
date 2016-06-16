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
    fd = None
    db = None
    kv = set()
    watch = dict()
    acks = collections.deque()
    peers = dict()
    followers = dict()
    quorum = 0
    state = ''
    minfile = 0
    maxfile = 0
    offset = 0
    size_prev = 0
    size = 0
    scan_checksum = ''
    append_checksum = ''
    node = None
    commit_time = 0
    start_time = time.time()

    @classmethod
    def json(cls):
        return json.dumps(dict(
            uptime=int(time.time()-g.start_time),
            state=g.state,
            node=g.node,
            minfile=g.minfile,
            maxfile=g.maxfile,
            offset=g.offset,
            size_prev=g.size_prev,
            size=g.size,
            pending=[len(g.kv), len(g.acks), len(g.watch)],
            peers=dict([(k, dict(
                minfile=v['minfile'],
                maxfile=v['maxfile'],
                offset=v['offset'],
                size=v['size'],
                size_prev=v['size_prev'],
                state=v['state'])) for k, v in g.peers.iteritems() if v])))


def vote(src, buf):
    g.peers[src] = json.loads(buf)

    if g.state:
        return

    if g.peers[src]['maxfile'] == g.maxfile:
        if g.peers[src]['size_prev'] > g.size_prev > 0:
            os.remove(os.path.join(g.opt.data, str(g.maxfile)))
            log('REMOVED file(%d) as peer(%s) prev size(%d > %d)',
                g.maxfile, src, g.peers[src]['size_prev'], g.size_prev)
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

        log('sent replication-request to(%s) file(%d) offset(%d) size(%d) as '
            '%s', leader, g.maxfile, g.offset, g.size, reason)
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

    log('received replication-request from(%s) file(%d) offset(%d) size(%d)',
        src, req['maxfile'], req['offset'], req['size'])

    if g.state.startswith('following-'):
        log('rejecting replication-request from(%s)', src)
        raise Exception('reject-replication-request')

    if src not in g.followers:
        log('accepted (%s) as follower(%d)', src, len(g.followers)+1)

    g.followers[src] = req

    if not g.state and len(g.followers) >= g.quorum:
        g.state = 'old-sync'
        log('state(OLD-SYNC) file(%d) offset(%d) followers(%d)',
            g.maxfile, g.offset, len(g.followers))

    in_sync = filter(lambda k: g.maxfile == g.followers[k]['maxfile'],
                     filter(lambda k: g.offset == g.followers[k]['offset'],
                            filter(lambda k: g.followers[k], g.followers)))

    if 'new-sync' == g.state and len(in_sync) >= g.quorum:
        g.state = 'leader'
        log('state(LEADER) msec(%d) file(%d) offset(%d) in_sync(%d)',
            (time.time()-g.start_time)*1000, g.maxfile, g.offset, len(in_sync))

        msgs = list()
        for f in set(g.peers) - set(g.followers):
            log('sent leader-conflict to non-follower(%s)', f)
            msgs.append(dict(dst=f, msg='leader_conflict'))
        return msgs + get_replication_responses()

    if 'old-sync' == g.state and len(in_sync) >= g.quorum:
        g.size_prev = g.offset
        g.maxfile += 1
        g.offset = 0

        append(''.join([struct.pack('!Q', 0),
                        struct.pack('!Q', g.maxfile),
                        struct.pack('!Q', 20),
                        '%20d' % g.size_prev]))

        scan()
        g.state = 'new-sync'
        log('state(NEW-SYNC) file(%d) offset(%d) in_sync(%d)',
            g.maxfile-1, g.size_prev, len(in_sync))
        if 0 == int(time.time()) % 3:
            log('exiting to force test')
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
        scan(g.maxfile, committed_offset)
        for k in g.watch.keys():
            f, o, src, buf = g.watch[k]
            if g.maxfile > f or (g.maxfile == f and g.offset > o):
                g.watch.pop(k)
                acks.append(get(src, buf))

    return acks + get_replication_responses()


def get_replication_responses():
    msgs = list()
    for src in filter(lambda k: g.followers[k], g.followers.keys()):
        req = g.followers[src]

        if 0 == req['maxfile']:
            f = map(int, os.listdir(g.opt.data))
            if f:
                log('sent replication-nextfile to(%s) file(%d)', src, min(f))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(g.opt.data, str(req['maxfile']))):
            continue

        with open(os.path.join(g.opt.data, str(req['maxfile']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['size']:
                log('sent replication-truncate to(%s) file(%d) size(%d) '
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
                    'size(%d)', src, req['maxfile'], req['offset'], len(buf))

                g.followers[src] = None
                msgs.append(dict(dst=src, msg='replication_response', buf=buf))

    return msgs


def replication_truncate(src, buf):
    trunc = json.loads(buf)['truncate']

    log('received replication-truncate from(%s) size(%d)', src, trunc)

    f = os.open(os.path.join(g.opt.data, str(g.maxfile)), os.O_RDWR)
    n = os.fstat(f).st_size
    assert(trunc < n)
    os.ftruncate(f, trunc)
    os.close(f)

    log('file(%d) truncated(%d) original(%d)', g.maxfile, trunc, n)
    os._exit(0)


def replication_nextfile(src, buf):
    req = json.loads(buf)

    log('received replication-nextfile from(%s) filenum(%d)',
        src, req['filenum'])

    g.size_prev = g.size
    g.maxfile = req['filenum']
    g.offset = 0
    g.size = 0

    if g.fd:
        os.close(g.fd)
        g.fd = None

    log('sent replication-request to(%s) file(%d) offset(%d) size(%d)',
        src, g.maxfile, g.offset, g.size)

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
            fd = os.open(g.opt.data, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)

        assert(g.size == os.fstat(g.fd).st_size)
        assert(len(buf) == os.write(g.fd, buf))

        g.size = os.fstat(g.fd).st_size

        scan()

        log('sent replication-request to(%s) file(%d) offset(%d) size(%d)',
            src, g.maxfile, g.offset, g.size)

        return dict(msg='replication_request', buf=g.json())
    except:
        os._exit(0)


def on_message(src, msg, buf):
    if type(src) is tuple:
        try:
            assert(msg in ('get', 'put', 'state')), 'invalid command'
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

    g.peers.pop(src)

    if 'following-' + src == g.state:
        g.db.commit()
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


def get(src, buf):
    filenum = struct.unpack('!Q', buf[0:8])[0]
    offset = struct.unpack('!Q', buf[8:16])[0]

    if filenum != 2**64-1 or offset != 2**64-1:
        if g.maxfile <= filenum:
            if g.maxfile == filenum and g.offset < offset:
                g.watch[src] = (filenum, offset, src, buf)
                return

    keys = list()
    i = 16
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

    k_list = dict()
    v_list = dict()
    for begin_key, end_key in keys:
        rows = g.db.execute('''select key, file, offset, version, length
                               from data where key between ? and ?
                            ''', (begin_key, end_key)).fetchall()

        for r in rows:
            k, f, o, v, l = r
            if (f == filenum and o > offset) or (f > filenum):
                v_list[bytes(k)] = (f, o, v, l)
            else:
                k_list[bytes(k)] = (f, o, v, l)

    for key, (filenum, offset, version, length) in v_list.iteritems():
        result.append(struct.pack('!Q', len(key)))
        result.append(key)
        result.append(struct.pack('!Q', version))
        result.append(struct.pack('!Q', length))

        with open(os.path.join(g.opt.data, str(filenum)), 'rb') as fd:
            fd.seek(offset, 0)
            buf = fd.read(length)
            assert(len(buf) == length)
            result.append(buf)

    for key, (filenum, offset, version, length) in k_list.iteritems():
        result.append(struct.pack('!Q', len(key)))
        result.append(key)
        result.append(struct.pack('!Q', version))
        result.append(struct.pack('!Q', 0))

    return dict(dst=src, buf=''.join(result))


def put(src, buf):
    if g.offset > g.opt.max_size:
        log('max file size reached(%d > %d)', g.offset, g.opt.max_size)
        os._exit(0)

    i = 0
    keys = dict()
    buf_list = list()
    while i < len(buf):
        key_len = struct.unpack('!Q', buf[i:i+8])[0]
        key = buf[i+8:i+8+key_len]

        assert(key not in keys), 'duplicate key'
        assert(key not in g.kv), 'txn in progress'

        ver = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]

        val_len = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
        val = buf[i+24+key_len:i+24+key_len+val_len]

        row = g.db.execute('select version from data where key=?',
                           (sqlite3.Binary(key),)).fetchone()
        if (row and row[0] != ver) or (not row and 0 != ver):
            buf_list.append(struct.pack('!Q', key_len))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', row[0] if row else 0))

        keys[key] = (ver+1 if val else 0, val)

        i += 24 + key_len + val_len

    assert(i == len(buf)), 'invalid put request'
    assert(len(keys)), 'empty request'

    if buf_list:
        return dict(buf=''.join([struct.pack('!B', 1)] + buf_list))

    g.kv.update(keys)

    buf_list = list()
    for key in keys:
        buf_list.append(struct.pack('!Q', len(key)))
        buf_list.append(key)
        buf_list.append(struct.pack('!Q', keys[key][0]))
        buf_list.append(struct.pack('!Q', len(keys[key][1])))
        buf_list.append(keys[key][1])

    rows = g.db.execute('''select key, file, offset, version, length from data
                           where file < ?  order by file, offset limit ?
                        ''', (g.maxfile, len(keys)*2)).fetchall()

    for key, filenum, offset, version, length in rows:
        key = bytes(key)

        if key not in g.kv and key not in keys:
            with open(os.path.join(g.opt.data, str(filenum)), 'rb') as fd:
                fd.seek(offset)
                buf = fd.read(length)
                assert(len(buf) == length)

            buf_list.append(struct.pack('!Q', len(key)))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', version))
            buf_list.append(struct.pack('!Q', length))
            buf_list.append(buf)

    append(''.join(buf_list))

    g.acks.append((g.size, src, set(keys)))
    return get_replication_responses()


def append(buf):
    g.append_checksum = hashlib.sha1(g.append_checksum)
    g.append_checksum.update(buf)
    g.append_checksum = g.append_checksum.digest()

    if not g.fd:
        g.fd = os.open(os.path.join(g.opt.data, str(g.maxfile)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    os.write(g.fd, struct.pack('!Q', len(buf)) + buf + g.append_checksum)
    g.size = os.fstat(g.fd).st_size


def scan(e_filenum=2**64, e_offset=2**64):
    b_file, b_offset, n_files, n_size, n_keys = g.maxfile, g.offset, 0, 0, 0

    filenum, offset = g.maxfile, g.offset

    while True:
        try:
            if filenum > e_filenum:
                break

            if not os.path.isfile(os.path.join(g.opt.data, str(filenum))):
                break

            with open(os.path.join(g.opt.data, str(filenum)), 'rb') as fd:
                t = time.time()
                os.fsync(fd.fileno())
                log('fsync(%d) in msec(%d)', filenum, (time.time()-t)*1000)

                fd.seek(0, 2)
                total_size = fd.tell()
                fd.seek(offset)

                n_files += 1

                while(offset < total_size):
                    l = struct.unpack('!Q', fd.read(8))[0]

                    if l + 28 > e_offset:
                        break

                    buf = fd.read(l)
                    y = fd.read(20)

                    chksum = hashlib.sha1(g.scan_checksum)
                    chksum.update(buf)
                    assert(y == chksum.digest())

                    g.maxfile = filenum
                    g.offset = offset + len(buf) + 28
                    g.scan_checksum = chksum.digest()

                    i = 0
                    while i < len(buf):
                        key_len = struct.unpack('!Q', buf[i:i+8])[0]
                        key = sqlite3.Binary(buf[i+8:i+8+key_len])

                        ver = struct.unpack(
                            '!Q', buf[i+8+key_len:i+16+key_len])[0]
                        val_len = struct.unpack(
                            '!Q', buf[i+16+key_len:i+24+key_len])[0]

                        if key_len > 0 and val_len > 0:
                           g.db.execute('insert or replace into data '
                                'values(?,?,?,?,?)', (key, filenum,
                                offset+8+i+24+key_len, ver, val_len))
                        else:
                            g.db.execute('delete from data where key=?',(key,))


                        n_keys += 1
                        i += 24 + key_len + val_len

                    assert(i == len(buf))

                    g.db.execute('insert into txn values(?, ?)',
                                 (g.maxfile, g.offset))

                    if time.time() > g.commit_time + 1:
                        t = time.time()
                        g.db.commit()
                        log('db commit in msec(%d)', (time.time()-t)*1000)
                        g.commit_time = time.time()

                    n_size += len(buf) + 28
                    offset += len(buf) + 28

                assert(offset == total_size)

            filenum += 1
            offset = 0
        except:
            log('scan begin(%d, %d) end(%d, %d) file(%d) size(%d) keys(%d)',
                b_file, b_offset, g.maxfile, g.offset, n_files, n_size, n_keys)
            log(traceback.format_exc())
            time.sleep(10**6)

    log('scan begin(%d, %d) end(%d, %d) file(%d) size(%d) keys(%d)',
        b_file, b_offset, g.maxfile, g.offset, n_files, n_size, n_keys)


def init(peers, opt):
    g.opt = opt
    g.db = sqlite3.connect(g.opt.index)
    g.db.execute('''create table if not exists data(key blob primary key,
        file unsigned, offset unsigned, version unsigned, length unsigned)''')
    g.db.execute('create index if not exists data_1 on data(file, offset)')
    g.db.execute('''create table if not exists txn(file unsigned,
        offset unsigned, primary key(file, offset))''')

    g.quorum = int(len(peers)/2.0 + 0.6)

    if not os.path.isdir(g.opt.data):
        os.mkdir(g.opt.data)

    dfd = os.open(g.opt.data, os.O_RDONLY)
    fcntl.flock(dfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    files = map(int, os.listdir(g.opt.data))
    if not files:
        g.db.execute('delete from data')
        g.db.commit()
        return

    min_f, max_f = min(files), max(files)
    max_f_len = os.path.getsize(os.path.join(g.opt.data, str(max_f)))
    log('minfile(%d) maxfile(%d) maxfilelen(%d)', min_f, max_f, max_f_len)

    if max_f_len < 104:
        os.remove(os.path.join(g.opt.data, str(max_f)))
        log('removed file(%d) as it has no data', max_f)
        os._exit(0)

    try:
        g.minfile = min_f

        g.db.execute('delete from data where file < ?', (min_f,))
        g.db.execute('delete from txn where file < ?', (min_f,))
        g.db.execute('delete from txn where file > ?', (max_f,))
        g.db.execute('delete from txn where file = ? and offset > ?',
                     (max_f, max_f_len))

        row = g.db.execute('select file, offset from txn order by file desc, '
                           'offset desc limit 1').fetchone()
        if row:
            g.db.execute('delete from data where file > ?', (row[0],))
            g.db.execute('delete from data where file = ? and offset > ?',
                         (row[0], row[1]))

        if row:
            g.maxfile, g.offset = row
            with open(os.path.join(g.opt.data, str(g.maxfile)), 'rb') as fd:
                fd.seek(g.offset - 20)
                g.scan_checksum = fd.read(20)
                assert(20 == len(g.scan_checksum))
        else:
            g.maxfile = min_f

            with open(os.path.join(g.opt.data, str(g.minfile)), 'rb') as fd:
                hdrlen = struct.unpack('!Q', fd.read(8))[0]
                fd.read(hdrlen)
                g.scan_checksum = fd.read(20)
                g.offset = fd.tell()
                assert(g.offset == hdrlen + 28)

        scan()
        g.size = g.offset
        g.append_checksum = g.scan_checksum

        row = g.db.execute('select min(file) from data').fetchone()
        g.minfile = row[0] if row[0] is not None else g.maxfile

        for n in range(min_f, g.minfile) + range(g.maxfile+1, max_f+1):
            os.remove(os.path.join(g.opt.data, str(n)))
            log('removed file({0})'.format(n))

        f = os.open(os.path.join(g.opt.data, str(g.maxfile)), os.O_RDWR)
        n = os.fstat(f).st_size
        if n > g.offset:
            os.ftruncate(f, g.offset)
            os.fsync(f)
            log('file(%d) truncated(%d) original(%d)', g.maxfile, g.offset, n)
        os.close(f)

        os.fsync(dfd)
        g.db.commit()

        if g.minfile < g.maxfile:
            g.size_prev = os.path.getsize(
                os.path.join(g.opt.data, str(g.maxfile-1)))
        log('initialized min(%d) max(%d) offset(%d) size(%d) prev(%d)',
            g.minfile, g.maxfile, g.offset, g.size, g.size_prev)
    except:
        log(traceback.format_exc())
        time.sleep(10**6)
