import os
import time
import json
import struct
import hashlib
import sqlite3
import traceback


class Globals():
    def __init__(self):
        self.port = None
        self.quorum = None
        self.leader = None
        self.followers = set()
        self.stats = None
        self.peers = None
        self.index = 'index'
        self.data = 'data'
        self.size = 128*1024*1024
        self.filenum = 0
        self.offset = 0
        self.total_size = 0
        self.checksum = ''
        self.commit_timestamp = int(time.time())
        self.fd = None


g = Globals()


def log(msg):
    timestamp = time.time()
    os.write(2, '{0}.{1} : {2}\n'.format(
        time.strftime('%y%m%d.%H%M%S', time.gmtime(timestamp)),
        '%06d' % ((timestamp - int(timestamp)) * 10**6),
        msg))


def scan(path, from_file, from_offset, checksum, to_file, to_offset, callback):
    total_size = 0
    file_closed = False
    for filenum in range(from_file, to_file+1):
        with open(os.path.join(path, str(filenum))) as fd:
            fd.seek(0, 2)
            total_size = fd.tell()

            offset = from_offset if(filenum == from_file) else 0
            fd.seek(offset)

            try:
                while(offset < total_size):
                    b = fd.read(8)
                    assert(len(b) == 8)

                    x = fd.read(struct.unpack('!Q', b)[0])
                    y = fd.read(20)
                    assert(len(y) == 20)

                    if (0 == len(x)) and (offset != 0):
                        file_closed = True

                    chksum = hashlib.sha1(checksum)
                    chksum.update(x)
                    assert(y == chksum.digest())
                    checksum = y

                    i = 0
                    while i < len(x):
                        key = x[i:i+32]
                        length = struct.unpack('!Q', x[i+32:i+40])[0]
                        ofst = offset+8+i+40
                        i += 40 + length

                        callback(key, filenum, ofst, length)

                    offset += len(x) + 28
                    assert(offset <= total_size)

                    log('scanned file({0}) offset({1})'.format(
                        filenum, fd.tell()))
            except:
                traceback.print_exc()
                break

    return (filenum, offset, checksum, total_size, file_closed)


def callback_stats_request(src, buf):
    msg = json.dumps(dict(
        filenum=g.filenum,
        leader=g.leader,
        offset=g.offset,
        netstats=g.stats))
    return dict(type='stats_response', buf=msg)


def callback_stats_response(src, buf):
    assert(src in g.peers)
    g.peers[src] = json.loads(buf)

    msgs = [dict(type='stats_request')]

    if not g.leader:
        if 'self' == g.peers[src]['leader']:
            g.leader = src
            msgs.append(dict(dst=src, type='leader_request'))
            log('sent leader-request to the leader {0}'.format(src))
        else:
            leader = (g.port, dict(filenum=g.filenum, offset=g.offset), 'self')
            count = 0
            for k, v in g.peers.iteritems():
                if v:
                    count += 1
                    if leader[1]['filenum'] != v['filenum']:
                        if leader[1]['filenum'] < v['filenum']:
                            leader = (k, v, 'filenum')
                        continue

                    if leader[1]['offset'] != v['offset']:
                        if leader[1]['offset'] < v['offset']:
                            leader = (k, v, 'offset')
                        continue

                    if k > leader[0]:
                        leader = (k, v, 'address')

            if (src == leader[0]) and (count >= g.quorum):
                g.leader = src
                log('candidate is {0} due to ({1})'.format(src, leader[2]))
                msgs.append(dict(type='leader_request'))
                log('sent leader-request to candidate {0}'.format(src))

        if g.leader:
            while g.followers:
                p = g.followers.pop()
                msgs.append(dict(dst=p, type='leader_reject'))
                log('sent leader-reject to {0}'.format(p))

    return msgs


def callback_leader_request(src, buf):
    log('received leader-request from {0}'.format(src))

    if g.leader and ('self' != g.leader):
        log('sent leader-rejected to {0}'.format(src))
        return dict(type='leader_reject')

    g.followers.add(src)

    if 'self' == g.leader:
        log('sent leader-accept to {0}'.format(src))
        return dict(type='leader_accept')

    if len(g.followers) >= g.quorum:
        g.leader = 'self'
        msgs = list()
        log('quorum reached({0}) for leader election'.format(g.quorum))
        for p in g.followers:
            msgs.append(dict(dst=p, type='leader_accept'))
            log('sent leader-accept to {0}'.format(p))

        return msgs


def callback_leader_reject(src, buf):
    g.leader = None
    log('received leader-reject from {0}'.format(src))


def callback_leader_accept(src, buf):
    assert(g.leader == src)
    log('received leader-accept from {0}'.format(src))


def callback_replication_request(src, buf):
    return [(src, 259, '')]


def callback_replication_response(src, buf):
    return [(src, 258, '')]


def callback_lockr_state_request(src, buf):
    state = dict()
    for ip, port in g.peers:
        state['{0}:{1}'.format(ip, port)] = g.peers[(ip, port)]

    return dict(buf=json.dumps(dict(
        filenum=g.filenum,
        offset=g.offset,
        leader=g.leader,
        netstats=g.stats,
        peers=state,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))))


def callback_random_packet(src, buf):
    import random
    log('received len({0})'.format(len(buf)))
    return dict(type='random_packet', buf=' '*(int(random.random()*100000000)))


def on_connect(src):
    return dict(type='stats_request'), dict(type='random_packet')


def on_disconnect(src):
    g.peers[src] = None

    if src == g.leader:
        log('leader {0} disconnected'.format(src))
        g.leader = None

    if src in g.followers:
        g.followers.remove(src)
        log('follower removed from {0}'.format(src))

    if ('self' == g.leader) and (len(g.followers) < g.quorum):
        log('relinquishing leadership as followers({0}) < quorum({1})'.format(
            len(g.followers), g.quorum))

        g.leader = None
        msgs = list()
        while g.followers:
            p = g.followers.pop()
            msgs.append(dict(dst=p, type='leader_reject'))
            log('leader reject sent to {0}'.format(p))

        return msgs


def on_accept(src):
    pass


def on_reject(src):
    if src in g.followers:
        g.followers.remove(src)
        log('removed follower{0} remaining({1})'.format(
            src, len(g.followers)))


def on_stats(stats):
    g.stats = stats


def callback_lockr_put_request(src, buf):
    docs = dict()
    i = 1
    while i < len(buf):
        length = struct.unpack('!Q', buf[i+48:i+56])[0]

        docs[buf[i:i+32]] = (
            struct.unpack('!Q', buf[i+32:i+40])[0],
            struct.unpack('!Q', buf[i+40:i+48])[0],
            buf[i+56:i+56+length])

        i += 56 + length

    assert(i == len(buf)), 'invalid put request'

    if g.offset > g.size:
        if g.fd:
            append(dict())
            os.close(g.fd)
        g.filenum += 1
        g.offset = 0
        g.fd = None

    if not g.fd:
        g.fd = os.open(
            os.path.join(g.data, str(g.filenum)),
            os.O_CREAT | os.O_WRONLY | os.O_APPEND)

        if 0 == g.offset:
            append(dict())

    try:
        append(docs)
        result = struct.pack('!B', 0)
    except:
        # traceback.print_exc()
        result = struct.pack('!B', 1)

    return dict(buf=result)


def callback_lockr_get_request(src, buf):
    result = list()
    i = 1
    while i < len(buf):
        key = buf[i:i+32]
        f, o, l = get(key)
        result.append(key)
        result.append(struct.pack('!Q', f))
        result.append(struct.pack('!Q', o))
        result.append(struct.pack('!Q', l))
        if l > 0:
            with open(os.path.join(g.data, str(f))) as fd:
                fd.seek(o)
                result.append(fd.read(l))
        i += 32

    return dict(buf=''.join(result))


def on_init(port, servers, conf_file):
    if not os.path.isdir(g.data):
        os.mkdir(g.data)

    g.peers = dict((ip_port, None) for ip_port in servers)
    g.quorum = int(len(g.peers)/2.0 + 0.6)
    g.port = port

    g.sqlite = sqlite3.connect(g.index)
    g.sqlite.execute('''create table if not exists docs
        (key blob primary key, file int, offset int, length int)''')

    result = g.sqlite.execute('''select file, offset, length
        from docs order by file desc, offset desc limit 1''').fetchall()
    if result:
        offset = result[0][1] + result[0][2]
        with open(os.path.join(g.data, str(result[0][0]))) as fd:
            fd.seek(offset)
            g.filenum = result[0][0]
            g.offset = offset+20
            g.checksum = fd.read(20)

    files = sorted([int(f) for f in os.listdir(g.data)])
    if files:
        if '' == g.checksum:
            with open(os.path.join(g.data, str(min(files)))) as fd:
                assert(0 == struct.unpack('!Q', fd.read(8))[0])
                g.filenum = min(files)
                g.offset = 28
                g.checksum = fd.read(20)

        g.filenum, g.offset, g.checksum, g.total_size, _ = scan(
            g.data, g.filenum, g.offset, g.checksum, max(files), 2**50, put)
        assert(max(files) == g.filenum)
        # with open(os.path.join(data_dir, str(self.file)), 'w') as fd:
        #     fd.truncate(self.offset)
        commit()


def commit():
    if int(time.time()) > g.commit_timestamp:
        g.sqlite.commit()
        g.commit_timestamp = int(time.time())


def put(key, filenum, offset, length):
    commit()
    g.sqlite.execute(
        'insert or replace into docs values(?, ?, ?, ?)',
        (sqlite3.Binary(key), filenum, offset, length))


def get(key):
    commit()
    result = g.sqlite.execute(
        'select file, offset, length from docs where key=?',
        (sqlite3.Binary(key),)).fetchall()
    return result[0] if result else (0, 0, 0)


def append(docs):
    buf_len = 0
    buf_list = list()
    for k, v in docs.iteritems():
        f, o, l = get(k)
        assert((v[0] == f) and (v[1] == o)), 'version mismatch'

        buf_list.append(k)
        buf_list.append(struct.pack('!Q', len(v[2])))
        buf_list.append(v[2])
        buf_len += 40 + len(v[2])

    offset = g.offset
    offset += 8
    for k, v in docs.iteritems():
        put(k, g.filenum, offset+32+8, len(v[2]))
        offset += 32 + 8 + len(v[2])

    chksum = hashlib.sha1(g.checksum)
    map(lambda b: chksum.update(b), buf_list)

    buf_list.insert(0, struct.pack('!Q', buf_len))
    buf_list.append(chksum.digest())

    os.write(g.fd, ''.join(buf_list))

    g.offset = offset + 20
    g.checksum = chksum.digest()
