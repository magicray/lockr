import os
import time
import json
import struct
import hashlib
import sqlite3
import logging
import traceback


class Globals():
    def __init__(self):
        self.port = None
        self.quorum = None
        self.leader = None
        self.followers = dict()
        self.stats = None
        self.peers = None
        self.index = 'index'
        self.data = 'data'
        self.size = 500#128*1024*1024
        self.filenum = 0
        self.offset = 0
        self.checksum = ''
        self.fd = None


g = Globals()


def callback_stats_request(src, buf):
    return dict(type='stats_response', buf=json.dumps(dict(
        filenum=g.filenum,
        leader=g.leader,
        offset=g.offset,
        netstats=g.stats,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))))


def callback_stats_response(src, buf):
    assert(src in g.peers)
    g.peers[src] = json.loads(buf)

    msgs = [dict(type='stats_request')]

    if g.leader:
        return msgs

    if 'self' == g.peers[src]['leader']:
        g.leader = src
        logging.critical('identified current leader{0}'.format(src))
    elif g.peers[src]['leader'] is None:
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
            logging.critical('leader({0}) selected due to ({1})'.format(
                src, leader[2]))

    if g.leader:
        if g.followers:
            logging.critical('exiting to reject followers({0})'.format(
                len(g.follwers)))
            exit(0)

        msgs.append(dict(type='replication_request',
                         buf=json.dumps(dict(filenum=g.filenum,
                                             offset=g.offset))))

        logging.critical(('sent replication-request to {0} file({1}) '
            'offset({2})').format(src, g.filenum, g.offset))

    return msgs


def callback_replication_request(src, buf):
    req = json.loads(buf)

    logging.critical(
        'received replication-request from{0} file({1}) offset({2})'.format(
            src, req['filenum'], req['offset']))

    if g.leader and 'self' != g.leader:
        logging.critical(('rejecting replication-request from{0} as '
            ' already following{1}').format(src, g.leader))
        raise Exception('reject-replication-request')

    g.followers[src] = json.loads(buf)
    logging.critical('accepted {0} as follower({1})'.format(
        src, len(g.followers)))

    if len(g.followers) == g.quorum:
        g.leader = 'self'
        logging.critical('assuming leadership as quorum reached({0})'.format(
            g.quorum))

    return

    filenum, offset = g.followers[src]

    with open(os.path.join(g.data, str(filenum))) as fd:
        fd.seek(offset)
        buf=fd.read(100*2**20)
        logging.critical(('sent replication-response to {0} file({1}) '
            'offset({2}) size({3})').format(src, filenum, offset, len(buf)))
        g.followers[src] = None
        return dict(type='replication_response', buf=buf)


def callback_replication_response(src, buf):
    assert(src == g.leader)
    logging.critical('received replication-response from {0} size({1})'.format(
        src, len(buf)))

    f = os.open(os.path.join(g.data, str(g.filenum)),
                os.O_CREAT| os.O_WRONLY | os.O_APPEND)

    assert(g.offset == os.fstat(f).st_size)
    assert(len(buf) == os.write(f, buf))
    assert(g.offset+len(buf) == os.fstat(f).st_size)

    os.fsync(f)
    os.close(f)

    if not g.checksum:
        with open(os.path.join(g.data, str(g.filenum))) as fd:
            fd.seek(8)
            g.checksum = fd.read(20)
            g.offset = 28

    g.filenum, g.offset, g.checksum, _ = scan(
        g.data, g.filenum, g.offset, g.checksum, g.filenum, 2**50, put)

    g.sqlite.commit()


def on_connect(src):
    logging.critical('connected to {0}'.format(src))
    return dict(type='stats_request')


def on_disconnect(src):
    g.peers[src] = None
    if src == g.leader:
        logging.critical('exiting as leader{0} disconnected'.format(src))
        exit(0)


def on_accept(src):
    logging.critical('accepted connection from {0}'.format(src))


def on_reject(src):
    logging.critical('terminated connection from {0}'.format(src))

    if src in g.followers:
        g.followers.pop(src)
        logging.critical('removed follower{0} remaining({1})'.format(
            src, len(g.followers)))

        if len(g.followers) < g.quorum:
            logging.critical(('relinquishing leadership as a followers({0}) < '
                'quorum({1})').format(len(g.followers), g.quorum))
            exit(0)


def on_stats(stats):
    g.stats = stats


def callback_lockr_state_request(src, buf):
    state = dict(self=dict(
        filenum=g.filenum,
        offset=g.offset,
        leader=g.leader,
        netstats=g.stats,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime())))

    for ip, port in g.peers:
        state['{0}:{1}'.format(ip, port)] = g.peers[(ip, port)]

    return dict(buf=json.dumps(state))


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
        f, o, l = index_get(key)
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


def on_init(port, servers, conf):
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
        with open(os.path.join(g.data, str(result[0][0]))) as fd:
            g.filenum = result[0][0]
            fd.seek(result[0][1] + result[0][2])
            g.checksum = fd.read(20)
            g.offset = fd.tell()
            assert(g.offset == result[0][1] + result[0][2] + 20)

    files = sorted([int(f) for f in os.listdir(g.data)])
    if files:
        if '' == g.checksum:
            with open(os.path.join(g.data, str(min(files)))) as fd:
                assert(0 == struct.unpack('!Q', fd.read(8))[0])
                g.filenum = min(files)
                g.offset = 28
                g.checksum = fd.read(20)

        g.filenum, g.offset, g.checksum = scan(g.data, g.filenum, g.offset,
                                               g.checksum, index_put)
        assert(max(files) == g.filenum)

        f = os.open(os.path.join(g.data, str(g.filenum)), os.O_RDWR)
        n = os.fstat(f).st_size
        assert(n >= g.offset)
        if n > g.offset:
            os.ftruncate(f, g.offset)
            os.fsync(f)
            logging.critical('file({0}) truncated({1}) original({2})'.format(
                g.filenum, g.offset, n))
        os.close(f)
        g.sqlite.commit()


def index_put(key, filenum, offset, length):
    g.sqlite.execute(
        'insert or replace into docs values(?, ?, ?, ?)',
        (sqlite3.Binary(key), filenum, offset, length))


def index_get(key):
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
        index_put(k, g.filenum, offset+32+8, len(v[2]))
        offset += 32 + 8 + len(v[2])

    chksum = hashlib.sha1(g.checksum)
    map(lambda b: chksum.update(b), buf_list)

    buf_list.insert(0, struct.pack('!Q', buf_len))
    buf_list.append(chksum.digest())

    os.write(g.fd, ''.join(buf_list))

    g.offset = offset + 20
    g.checksum = chksum.digest()


def scan(path, filenum, offset, checksum, callback):
    result = (filenum, offset, checksum)
    while True:
        try:
            with open(os.path.join(path, str(filenum))) as fd:
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

                    i = 0
                    while i < len(x):
                        key = x[i:i+32]
                        length = struct.unpack('!Q', x[i+32:i+40])[0]
                        ofst = offset+8+i+40
                        i += 40 + length

                        callback(key, filenum, ofst, length)

                    offset += len(x) + 28
                    assert(offset <= total_size)
                    assert(offset == fd.tell())

                    result = (filenum, offset, checksum)
                    logging.critical('scanned file({0}) offset({1})'.format(
                        filenum, offset))

            filenum += 1
            offset = 0
        except:
            return result
