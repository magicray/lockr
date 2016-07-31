import os
import sys
import json
import time
import copy
import fcntl
import msgio
import struct
import signal
import random
import sqlite3
import hashlib
import logging
import traceback
import collections

from logging import critical as log


class g:
    conf = None
    fd = None
    db = None
    kv = set()
    oldest = collections.deque()
    watch_key = dict()
    watch_src = dict()
    acks = collections.deque()
    conns = set()
    peers = dict()
    followers = dict()
    quorum = 0
    state = ''
    minfile = 1
    maxfile = 0
    offset = 0
    size_prev = 0
    size = 0
    clock = 0
    vclock = None
    scan_checksum = ''
    append_checksum = ''
    node = None
    commit_time = 0
    committed = 0
    uncommitted = collections.deque()
    version = 1
    start_time = None

    @classmethod
    def json(cls):
        g.vclock = read_hdr(g.maxfile)

        return json.dumps(dict(
            state=g.state,
            clock=g.clock,
            vclock=g.vclock,
            minfile=g.minfile,
            maxfile=g.maxfile,
            offset=g.offset,
            size_prev=g.size_prev,
            size=g.size,
            committed=g.committed))


def read_hdr(filenum):
    filename = os.path.join(g.conf['data'], str(filenum))
    if not os.path.isfile(filename):
        return dict()

    with open(os.path.join(g.conf['data'], str(filenum))) as fd:
        hdrlen = struct.unpack('!Q', fd.read(8))[0]
        buf = fd.read(hdrlen)
        assert(len(buf) == hdrlen)
        return json.loads(buf[32:])


def replication_connect(src, buf):
    g.peers[src] = json.loads(buf)

    if g.state:
        return

    if g.peers[src]['maxfile'] == g.maxfile:
        if g.peers[src]['size_prev'] > g.size_prev > 0:
            os.remove(os.path.join(g.conf['data'], str(g.maxfile)))
            log('REMOVED file(%d) as peer(%s) prev size(%d > %d)',
                g.maxfile, src, g.peers[src]['size_prev'], g.size_prev)
            os._exit(0)

    if g.maxfile > 0 and g.peers[src]['minfile'] > g.maxfile:
        log('src(%s) minfile(%d) > maxfile(%d)',
            src, g.peers[src]['minfile'], g.maxfile)
        while True:
            try:
                os.remove(os.path.join(g.conf['data'], str(g.maxfile)))
                log('removed file(%d)', g.maxfile)
                g.maxfile -= 1
            except:
                os._exit(0)

    m_vclock = read_hdr(g.maxfile)
    p_vclock = g.peers[src]['vclock']
    if g.peers[src]['maxfile'] == g.maxfile:
        for s in set(p_vclock).intersection(m_vclock):
            if m_vclock[s] == p_vclock[s]:
                continue

            log('mismatch vclock{0}'.format(json.dumps(m_vclock)))
            log('peer({0}) vclock{1}'.format(src, json.dumps(p_vclock)))

            if m_vclock[s] < p_vclock[s]:
                os.remove(os.path.join(g.conf['data'], str(g.maxfile)))
                log('REMOVED file(%d) due to vclock conflict', g.maxfile)
                os._exit(0)

            log('sent vclock-conflict to(%s)', src)
            return dict(dst=src, msg='vclock_conflict')

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


def vclock_conflict(src, buf):
    os.remove(os.path.join(g.conf['data'], str(g.maxfile)))
    log('REMOVED file(%d) due to replication_conflict', g.maxfile)
    os._exit(0)


def replication_request(src, buf):
    g.peers[src] = json.loads(buf)

    log('received replication-request from(%s) file(%d) offset(%d) size(%d)',
        src, g.peers[src]['maxfile'], g.peers[src]['offset'],
        g.peers[src]['size'])

    if g.state.startswith('following-'):
        log('rejecting replication-request from(%s)', src)
        raise Exception('reject-replication-request')

    m_vclock = read_hdr(g.peers[src]['maxfile'])
    p_vclock = g.peers[src]['vclock']
    if p_vclock and m_vclock != p_vclock:
        log('this : %s', m_vclock)
        log('peer : %s', p_vclock)
        log('sent vclock-conflict to(%s)', src)
        return dict(msg='vclock_conflict')

    if src not in g.followers:
        log('accepted (%s) as follower(%d)', src, len(g.followers)+1)

    g.followers[src] = True

    if not g.state and len(g.followers) >= g.quorum:
        g.state = 'old-sync'
        log('state(OLD-SYNC) file(%d) offset(%d) followers(%d)',
            g.maxfile, g.offset, len(g.followers))

    in_sync = filter(lambda k: g.maxfile == g.peers[k]['maxfile'],
                     filter(lambda k: g.offset == g.peers[k]['offset'],
                            filter(lambda k: g.followers[k], g.followers)))

    if 'new-sync' == g.state and len(in_sync) >= g.quorum:
        g.state = 'leader'
        g.committed = g.offset
        db_sync()

        log('state(LEADER) file(%d) offset(%d) in_sync(%d)',
            g.maxfile, g.offset, len(in_sync))

        msgs = list()
        for f in g.followers:
            log('sent replication-committed to(%s) commit(%d)', f, g.committed)
            msgs.append(dict(dst=f, msg='replication_committed', buf=g.json()))

        for f in set(g.peers) - set(g.followers):
            log('sent leader-conflict to(%s)', f)
            msgs.append(dict(dst=f, msg='leader_conflict'))

        return msgs + get_replication_responses()

    if 'old-sync' == g.state and len(in_sync) >= g.quorum:
        g.size_prev = g.offset
        g.maxfile += 1
        g.offset = 0

        vclk = dict([(f, g.peers[f]['clock']) for f in g.followers])
        vclk[g.node] = g.clock
        vclk = json.dumps(vclk, sort_keys=True, indent=4)
        append(''.join([struct.pack('!Q', 0),
                        struct.pack('!Q', g.version),
                        struct.pack('!Q', 0),
                        struct.pack('!Q', len(vclk)),
                        vclk]))
        scan()
        g.version += 1
        g.state = 'new-sync'
        log('state(NEW-SYNC) file(%d) offset(%d) in_sync(%d)',
            g.maxfile-1, g.size_prev, len(in_sync))
        if 0 == int(time.time()) % 3:
            log('exiting to force test')
            os._exit(0)

        return get_replication_responses()

    acks = list()
    triggered = set()
    if 'leader' == g.state:
        offsets = list()
        for k, v in g.peers.iteritems():
            if v and v['maxfile'] == g.maxfile:
                offsets.append(v['offset'])

        if len(offsets) >= g.quorum:
            g.committed = sorted(offsets, reverse=True)[g.quorum-1]

            while g.acks:
                if g.acks[0][0] > g.committed:
                    break

                offset, dst, keys = g.acks.popleft()
                for k in keys:
                    g.kv.remove(k)
                    triggered.update(g.watch_key.pop(k, set()))

                acks.append(dict(dst=dst, buf=''.join([
                    struct.pack('!B', 0),
                    struct.pack('!Q', g.maxfile),
                    struct.pack('!Q', offset)])))

    if acks:
        scan()
        db_sync()

        for p in g.followers:
            log('sent replication-committed to(%s) commit(%d)', p, g.committed)
            acks.append(dict(dst=p, msg='replication_committed', buf=g.json()))

        for src in triggered:
            filenum, keys = g.watch_src[src]
            for k in keys:
                if k in g.watch_key:
                    g.watch_key[k].remove(src)

            acks.append(get_process(filenum, src, keys))

    return acks + get_replication_responses()


def get_replication_responses():
    data_dir = g.conf['data']

    msgs = list()
    for src in filter(lambda k: g.followers[k], g.followers):
        req = g.peers[src]

        if 0 == req['maxfile']:
            f = map(int, os.listdir(data_dir))
            if f:
                log('sent replication-nextfile to(%s) file(%d)', src, min(f))

                g.followers[src] = False
                msgs.append(dict(dst=src, msg='replication_nextfile',
                                 buf=json.dumps(dict(filenum=min(f)))))

        if not os.path.isfile(os.path.join(data_dir, str(req['maxfile']))):
            continue

        with open(os.path.join(data_dir, str(req['maxfile']))) as fd:
            fd.seek(0, 2)

            if fd.tell() < req['size']:
                log('sent replication-truncate to(%s) file(%d) size(%d) '
                    'truncate(%d)',
                    src, req['maxfile'], req['size'], fd.tell())

                g.followers[src] = False
                msgs.append(dict(dst=src, msg='replication_truncate',
                                 buf=json.dumps(dict(truncate=fd.tell()))))

            if fd.tell() == req['size']:
                if g.maxfile == req['maxfile']:
                    continue

                log('sent replication-nextfile to(%s) file(%d)',
                    src, req['maxfile']+1)

                g.followers[src] = False
                msgs.append(dict(dst=src, msg='replication_nextfile',
                            buf=json.dumps(dict(filenum=req['maxfile']+1))))

            fd.seek(req['size'])
            buf = fd.read(g.conf['repl_size'])
            if buf:
                log('sent replication-response to(%s) file(%d) offset(%d) '
                    'size(%d)', src, req['maxfile'], req['size'], len(buf))

                g.followers[src] = False
                msgs.append(dict(dst=src, msg='replication_response', buf=buf))

    return msgs


def replication_truncate(src, buf):
    trunc = json.loads(buf)['truncate']

    log('received replication-truncate from(%s) size(%d)', src, trunc)

    assert(trunc > 0)
    f = os.open(os.path.join(g.conf['data'], str(g.maxfile)), os.O_RDWR)
    n = os.fstat(f).st_size
    assert(trunc < n)
    os.ftruncate(f, trunc)
    os.close(f)

    log('file(%d) truncated(%d) original(%d)', g.maxfile, trunc, n)
    os._exit(0)


def replication_committed(src, buf):
    log('received replication-committed from(%s)', src)
    assert(g.state.endswith('-' + src))

    g.peers[src] = json.loads(buf)

    if g.maxfile == g.peers[src]['maxfile']:
        if g.offset >= g.peers[src]['committed']:
            g.committed = g.peers[src]['committed']
            db_sync()


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
            g.fd = os.open(os.path.join(g.conf['data'], str(g.maxfile)),
                           os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                           0644)
            fd = os.open(g.conf['data'], os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)

        assert(g.size == os.fstat(g.fd).st_size)
        assert(len(buf) == os.write(g.fd, buf))
        assert(g.size + len(buf) == os.fstat(g.fd).st_size)

        g.size = os.fstat(g.fd).st_size

        if g.maxfile > 1 and not g.scan_checksum:
            log('exiting to reinitialize checksum')
            os._exit(0)

        scan()

        assert(g.size == os.fstat(g.fd).st_size)

        log('sent replication-request to(%s) file(%d) offset(%d) size(%d)',
            src, g.maxfile, g.offset, g.size)

        return dict(msg='replication_request', buf=g.json())
    except:
        os._exit(0)


def on_message(src, msg, buf):
    g.conns.add(src)

    if type(src) is tuple:
        try:
            assert(msg in ('get', 'put', 'state')), 'invalid command'
            return globals()[msg](src, buf)
        except:
            return dict(buf=struct.pack('!B', 255) + traceback.format_exc())

    return globals()[msg](src, buf)


def on_connect(src):
    return dict(msg='replication_connect', buf=g.json())


def on_disconnect(src, exc, tb):
    if src in g.conns:
        g.conns.remove(src)

    if exc and str(exc) != 'reject-replication-request':
        log(tb)

    if src not in g.peers:
        _, keys = g.watch_src.pop(src, (0, dict()))
        for k in keys:
            g.watch_key[k].remove(src)
            if not g.watch_key[k]:
                del(g.watch_key[k])
        return

    g.peers.pop(src)

    if 'following-' + src == g.state:
        log('exiting as LEADER({0}) disconnected'.format(src))
        os._exit(0)

    if src in g.followers:
        del(g.followers[src])
        log('removed follower(%s) remaining(%d)', src, len(g.followers))

    if g.state in ('old-sync', 'new-sync', 'leader'):
        if len(g.followers) < g.quorum:
            log('exiting as followers(%d) < quorum(%d)',
                len(g.followers), g.quorum)
            os._exit(0)


def state(src, buf):
    d = copy.deepcopy(g.peers)
    d.update(json.loads(g.json()))
    d.update(dict(uptime=int(time.time()-g.start_time),
                  node=g.node,
                  total_clients=len(g.conns),
                  pending_keys=len(g.kv),
                  pending_clients=len(g.acks),
                  watch_keys=len(g.watch_key),
                  watch_clients=len(g.watch_src),
                  version=g.version))

    return dict(buf=json.dumps(d))


def get(src, buf):
    filenum = struct.unpack('!Q', buf[0:8])[0]
    offset = struct.unpack('!Q', buf[8:16])[0]

    i = 16
    keys = dict()
    while i < len(buf):
        key_len = struct.unpack('!Q', buf[i:i+8])[0]
        key = buf[i+8:i+8+key_len]
        version = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]

        keys[key] = version

        i += 16 + key_len

    assert(i == len(buf))

    if filenum > g.maxfile or (filenum == g.maxfile and offset > g.committed):
        for k in keys:
            g.watch_key.setdefault(k, set()).add(src)
        g.watch_src[src] = (filenum, keys)
        return

    return get_process(filenum, src, keys)


def get_process(filenum, src, keys):
    result = [struct.pack('!B', 0 if filenum < g.minfile else 1),
              struct.pack('!Q', g.maxfile),
              struct.pack('!Q', g.offset)]

    for key, version in keys.iteritems():
        row = g.db.execute('''select file, offset, version, length
                              from data where key = ?
                           ''', (sqlite3.Binary(key),)).fetchone()

        f, o, v, l = row if row else (0, 0, 0, 0)
        v = v if l > 0 else 0

        if version != v:
            result.append(struct.pack('!Q', len(key)))
            result.append(key)
            result.append(struct.pack('!Q', v))

            if v > version:
                result.append(struct.pack('!Q', l))
                with open(os.path.join(g.conf['data'], str(f)), 'rb') as fd:
                    fd.seek(o)
                    b = fd.read(l)
                    assert(len(b) == l)
                    result.append(b)

    return dict(dst=src, buf=''.join(result))


def put(src, buf):
    if g.offset > g.conf['max_size']:
        log('max file size reached(%d > %d)', g.offset, g.conf['max_size'])
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
        ttl = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]

        val_len = struct.unpack('!Q', buf[i+24+key_len:i+32+key_len])[0]
        val = buf[i+32+key_len:i+32+key_len+val_len]

        row = g.db.execute('''select version from data
                              where key = ? and length > 0
                           ''', (sqlite3.Binary(key),)).fetchone()
        if (row and row[0] != ver) or (not row and 0 != ver):
            buf_list.append(struct.pack('!Q', key_len))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', row[0] if row else 0))

        keys[key] = (ttl, val)

        i += 32 + key_len + val_len

    assert(i == len(buf)), 'invalid put request'
    assert(len(keys)), 'empty request'

    if buf_list:
        return dict(buf=''.join([struct.pack('!B', 1)] + buf_list))

    g.kv.update(keys)

    buf_list = list()
    for key, (ttl, val) in keys.iteritems():
        ttl = ttl if 0 == ttl else int(time.time() + ttl)
        buf_list.append(struct.pack('!Q', len(key)))
        buf_list.append(key)
        buf_list.append(struct.pack('!Q', g.version))
        buf_list.append(struct.pack('!Q', ttl))
        buf_list.append(struct.pack('!Q', len(val)))
        buf_list.append(val)

    count = 0
    while g.oldest and count < len(keys):
        key, filenum, offset, version, length = g.oldest.popleft()

        row = g.db.execute('select file, offset from data where key = ?',
                           (sqlite3.Binary(key),)).fetchone()
        if row[0] != filenum or row[1] != offset:
            continue

        if key not in g.kv and key not in keys and length > 0:
            buf_list.append(struct.pack('!Q', len(key)))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', version))
            buf_list.append(struct.pack('!Q', ttl))
            buf_list.append(struct.pack('!Q', length))

            with open(os.path.join(g.conf['data'], str(filenum)), 'rb') as fd:
                fd.seek(offset)
                buf = fd.read(length)
                assert(len(buf) == length)
                buf_list.append(buf)

            count += 1

    append(''.join(buf_list))
    g.version += 1

    g.acks.append((g.size, src, set(keys)))
    return get_replication_responses()


def append(buf):
    g.append_checksum = hashlib.sha1(g.append_checksum)
    g.append_checksum.update(buf)
    g.append_checksum = g.append_checksum.digest()

    if not g.fd:
        g.fd = os.open(os.path.join(g.conf['data'], str(g.maxfile)),
                       os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                       0644)

    os.write(g.fd, struct.pack('!Q', len(buf)) + buf + g.append_checksum)
    g.size = os.fstat(g.fd).st_size


def db_sync():
    count = 0
    while g.uncommitted:
        commit = True
        for key, filenum, offset, version, ttl, length in g.uncommitted[0]:
            if filenum == g.maxfile and offset > g.committed:
                commit = False
                break

        if commit is not True:
            break

        txn = g.uncommitted.popleft()
        for key, filenum, offset, version, ttl, length in txn:
            g.db.execute('insert or replace into data values(?,?,?,?,?,?)',
                         (key, filenum, offset, version, ttl, length))
            count += 1

    if time.time() > g.commit_time:
        t = time.time()
        g.db.commit()
        t = time.time() - t
        log('db sync count(%d) msec(%d)', count, t*1000)
        g.commit_time = time.time() + t*10
    else:
        log('db_sync count(%d)', count)


def scan():
    b_file, b_offset, n_files, n_size, n_keys = g.maxfile, g.offset, 0, 0, 0

    filenum, offset = g.maxfile, g.offset

    while True:
        try:
            if not os.path.isfile(os.path.join(g.conf['data'], str(filenum))):
                break

            with open(os.path.join(g.conf['data'], str(filenum)), 'rb') as fd:
                t = time.time()
                os.fsync(fd.fileno())
                log('fsync(%d) in msec(%d)', filenum, (time.time()-t)*1000)

                fd.seek(0, 2)
                total_size = fd.tell()
                fd.seek(offset)

                n_files += 1

                while(offset < total_size):
                    hdr = fd.read(8)
                    if 8 != len(hdr):
                        break

                    l = struct.unpack('!Q', hdr)[0]

                    buf = fd.read(l)
                    if l != len(buf):
                        break

                    y = fd.read(20)
                    if 20 != len(y):
                        break

                    chksum = hashlib.sha1(g.scan_checksum)
                    chksum.update(buf)
                    assert(y == chksum.digest())

                    g.maxfile = filenum
                    g.offset = offset + len(buf) + 28
                    g.scan_checksum = chksum.digest()

                    i = 0
                    txn = list() 
                    while i < len(buf):
                        key_len = struct.unpack('!Q', buf[i:i+8])[0]
                        key = sqlite3.Binary(buf[i+8:i+8+key_len])

                        ver = struct.unpack(
                            '!Q', buf[i+8+key_len:i+16+key_len])[0]
                        ttl = struct.unpack(
                            '!Q', buf[i+16+key_len:i+24+key_len])[0]
                        val_len = struct.unpack(
                            '!Q', buf[i+24+key_len:i+32+key_len])[0]

                        txn.append((key, filenum, offset+8+i+32+key_len,
                                    ver, ttl, val_len))

                        n_keys += 1
                        i += 32 + key_len + val_len

                    assert(i == len(buf))

                    g.uncommitted.append(txn)

                    n_size += len(buf) + 28
                    offset += len(buf) + 28

            filenum += 1
            offset = 0
        except:
            signal.alarm(0)
            log('scan begin(%d, %d) end(%d, %d) file(%d) size(%d) keys(%d)',
                b_file, b_offset, g.maxfile, g.offset, n_files, n_size, n_keys)
            log(traceback.format_exc())
            time.sleep(10**8)

    log('scan begin(%d, %d) end(%d, %d) file(%d) size(%d) keys(%d)',
        b_file, b_offset, g.maxfile, g.offset, n_files, n_size, n_keys)


def init(conf):
    g.conf = conf
    g.db = sqlite3.connect(g.conf['index'])
    g.db.execute('''create table if not exists data(key blob, file unsigned,
        offset unsigned, version unsigned, ttl unsigned, length unsigned,
        primary key(key))''')
    g.db.execute('create unique index if not exists fo on data(file, offset)')
    g.db.execute('create index if not exists v on data(version)')
    g.db.execute('create index if not exists t on data(ttl)')
    g.db.execute('''create table if not exists kv(key text primary key,
        value int)''')

    g.quorum = int(len(conf['nodes'])/2.0 + 0.6)
    g.node = '{0}:{1}'.format(*conf['port'])

    data_dir = g.conf['data']
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    dfd = os.open(data_dir, os.O_RDONLY)
    fcntl.flock(dfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    row = g.db.execute("select value from kv where key='clock'").fetchone()
    g.clock = row[0] + 1 if(row) else 1
    g.db.execute("insert or replace into kv values('clock', ?)", (g.clock,))

    files = map(int, os.listdir(data_dir))
    if not files:
        g.db.execute('delete from data')
        g.db.commit()
        return

    g.minfile, g.maxfile = min(files), max(files)
    g.size = os.path.getsize(os.path.join(data_dir, str(g.maxfile)))
    log('minfile(%d) maxfile(%d) maxfilelen(%d)', g.minfile, g.maxfile, g.size)

    g.db.execute('''delete from data where
                    (file < ?) or (file > ?) or (file = ? and offset > ?)
                 ''', (g.minfile, g.maxfile, g.maxfile, g.size))
    g.db.execute('delete from data where ttl between 1 and ?', (time.time(),))

    row = g.db.execute('''select file, offset, length from data
        order by file desc, offset desc limit 1''').fetchone()
    if row:
        g.maxfile, g.offset = row[0], row[1] + row[2] + 20

        with open(os.path.join(data_dir, str(g.maxfile)), 'rb') as fd:
            fd.seek(g.offset - 20)
            g.scan_checksum = fd.read(20)
            assert(20 == len(g.scan_checksum))
    else:
        g.maxfile = g.minfile

        with open(os.path.join(data_dir, str(g.minfile)), 'rb') as fd:
            hdrlen = struct.unpack('!Q', fd.read(8))[0]
            fd.read(hdrlen)
            g.scan_checksum = fd.read(20)
            g.offset = fd.tell()
            assert(g.offset == hdrlen + 28)

    scan()

    if g.size > g.offset:
        assert(g.offset > 0)
        f = os.open(os.path.join(data_dir, str(g.maxfile)), os.O_RDWR)
        os.ftruncate(f, g.offset)
        os.fsync(f)
        os.close(f)
        log('file(%d) truncated(%d) original(%d)', g.maxfile, g.offset, g.size)

    g.size = g.offset
    g.append_checksum = g.scan_checksum
    g.db.commit()

    row = g.db.execute('select max(version) from data').fetchone()
    if row[0]:
        g.version = row[0] + 1

        for n in range(g.minfile, g.maxfile-1):
            rows = g.db.execute('''select count(*) from data
                                   where file = ? and length > 0''', (n,))

            if rows.fetchone()[0] > 0:
                break

            os.remove(os.path.join(data_dir, str(n)))
            log('removed file(%d) containing overwritten data', n)
            os.fsync(dfd)
            g.minfile = n + 1

    if g.minfile < g.maxfile:
        g.size_prev = os.path.getsize(os.path.join(data_dir, str(g.maxfile-1)))

    rows = g.db.execute('''select key, file, offset, version, length
                           from data where file < ?  order by file, offset
                        ''', (min(g.minfile+2, g.maxfile-2),))
    for r in rows:
        g.oldest.append((bytes(r[0]), r[1], r[2], r[3], r[4]))

    log('initialized min(%d) max(%d) offset(%d) size(%d) prev(%d)',
        g.minfile, g.maxfile, g.offset, g.size, g.size_prev)


if __name__ == '__main__':
    logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

    conf = dict(key='key',
                cert='ssl.cert',
                data='data',
                index='index.db',
                max_size=2**14,
                repl_size=2**20,
                timeout=600000,
                port='127.0.0.1:2001',
                nodes=['127.0.0.1:{0}'.format(p) for p in range(2002, 2006)])

    if not os.path.isfile('conf.json'):
        print('Create conf.json using the following sample:')
        print(json.dumps(conf, indent=4, sort_keys=4))
        exit(1)

    fcntl.flock(os.open('.', os.O_RDONLY), fcntl.LOCK_EX | fcntl.LOCK_NB)

    if len(sys.argv) > 1:
        if os.fork():
            os._exit(0)

        os.setsid()

    os.close(0)
    os.close(1)

    while True:
        if 0 == os.fork():
            g.start_time = time.time()

            if len(sys.argv) > 1:
                os.dup2(os.open('{0}.{1}'.format(
                    sys.argv[1],
                    time.strftime('%y%m%d', time.gmtime())),
                    os.O_CREAT | os.O_WRONLY | os.O_APPEND), 2)

            logging.critical('')
            try:

                with open('conf.json') as fd:
                    conf.update(json.loads(fd.read()))

                conf['nodes'] = set(map(lambda x: (x.split(':')[0],
                                                   int(x.split(':')[1])),
                                        conf['nodes']))

                ip, port = conf['port'].split(':')
                conf['port'] = (ip, int(port))

                init(conf)

                signal.alarm(random.randint(conf['timeout'],
                                            2*conf['timeout']))

                msgio.loop(sys.modules[__name__],
                           conf['port'],
                           conf['nodes'],
                           conf['key'],
                           conf['cert'])
            except:
                signal.alarm(0)
                logging.critical(traceback.format_exc())
                time.sleep(10**8)

            os._exit(0)

        os.wait()
