import os
import time
import json
import struct
import hashlib
import sqlite3
import logging
import traceback


class g:
    port = None
    quorum = None
    leader = None
    followers = dict()
    stats = None
    peers = None
    index = 'index'
    data = 'data'
    max_file_size = 256
    state = dict(filenum=0, offset=0, checksum='', size=0, eof=False)


def stats_request(src, buf):
    if g.leader and 'self' != g.leader and src in g.followers:
        g.followers.pop(src)
        logging.critical(('disconnecting follower{0} as already '
                          'following{1}').format(src, g.leader))
        raise Exception('kill-follower')

    return dict(msg='stats_response', buf=json.dumps(dict(
        state=g.state,
        leader=g.leader,
        stats=g.stats,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))))


def stats_response(src, buf):
    assert(src in g.peers)
    g.peers[src] = json.loads(buf)

    msgs = [dict(msg='stats_request')]

    if g.leader:
        return msgs

    if 'self' == g.peers[src]['leader']:
        g.leader = src
        logging.critical('identified current leader{0}'.format(src))
    elif g.peers[src]['leader'] is None:
        leader = (g.port, g.state, 'self')
        count = 0
        for k, v in g.peers.iteritems():
            if v:
                count += 1
                l, p = leader[1], v['state']

                if l['filenum'] != p['filenum']:
                    if l['filenum'] < p['filenum']:
                        leader = (k, p, 'filenum({0} > {1})'.format(
                            p['filenum'], l['filenum']))
                    continue

                if l['eof'] and not p['eof']:
                    continue

                if p['eof'] and not l['eof']:
                    leader = (k, p, 'eof({0}, {1})'.format(
                        p['offset'], l['offset']))
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
            g.leader = src
            logging.critical('leader({0}) selected due to {1}'.format(
                src, leader[2]))

    if g.leader:
        msgs.append(dict(msg='replication_request', buf=json.dumps(g.state)))

        logging.critical('LEADER{0} identified'.format(src))
        logging.critical(
            'sent replication-request to{0} file({1}) offset({2})'.format(
                src, g.state['filenum'], g.state['offset']))

    return msgs


def replication_request(src, buf):
    req = json.loads(buf)

    logging.critical(
        'received replication-request from{0} file({1}) offset({2})'.format(
            src, req['filenum'], req['offset']))

    if g.leader and 'self' != g.leader:
        logging.critical(('rejecting replication-request from{0} as already '
                          'following{1}').format(src, g.leader))
        raise Exception('reject-replication-request')

    g.followers[src] = json.loads(buf)
    logging.critical('accepted {0} as follower({1})'.format(
        src, len(g.followers)))

    if len(g.followers) == g.quorum:
        g.leader = 'self'
        logging.critical('assuming LEADERSHIP as quorum reached({0})'.format(
            g.quorum))

        g.state['filenum'] += 1
        g.state['offset'] = 0
        append('')

    return

    filenum, offset = g.followers[src]

    with open(os.path.join(g.data, str(filenum))) as fd:
        fd.seek(offset)
        buf = fd.read(100*2**20)
        logging.critical(('sent replication-response to {0} file({1}) '
                          'offset({2}) size({3})').format(
                              src, filenum, offset, len(buf)))
        g.followers[src] = None
        return dict(msg='replication_response', buf=buf)


def replication_response(src, buf):
    assert(src == g.leader)
    logging.critical('received replication-response from {0} size({1})'.format(
        src, len(buf)))

    f = os.open(os.path.join(g.data, str(g.filenum)),
                os.O_CREAT | os.O_WRONLY | os.O_APPEND)

    assert(g.offset == os.fstat(f).st_size)
    assert(len(buf) == os.write(f, buf))
    assert(g.offset+len(buf) == os.fstat(f).st_size)

    os.fsync(f)
    os.close(f)

    if not g.checksum:
        with open(os.path.join(g.data, str(g.filenum))) as fd:
            fd.seek(8)
            g.checksum = fd.read(20).encode('hex')
            g.offset = 28

    g.filenum, g.offset, g.checksum, _ = scan(
        g.data, g.filenum, g.offset, g.checksum, g.filenum, 2**50, put)

    g.sqlite.commit()


def on_connect(src):
    logging.critical('connected to {0}'.format(src))
    return dict(msg='stats_request')


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
                              'quorum({1})').format(
                                  len(g.followers), g.quorum))
            exit(0)


def on_stats(stats):
    g.stats = stats


def state(src, buf):
    state = dict([('{0}:{1}'.format(*k), v) for k, v in g.peers.iteritems()])

    state['self'] = dict(
        state=g.state,
        leader=g.leader,
        stats=g.stats,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))

    return dict(buf=json.dumps(state))


def put(src, buf):
    try:
        i = 0
        buf_list = list()
        while i < len(buf):
            key = buf[i:i+32]
            filenum = struct.unpack('!Q', buf[i+32:i+40])[0]
            offset = struct.unpack('!Q', buf[i+40:i+48])[0]
            length = struct.unpack('!Q', buf[i+48:i+56])[0]

            f, o, l = index_get(key)
            assert((f == filenum) and (o == offset)), 'version mismatch'

            buf_list.append(key)
            buf_list.append(struct.pack('!Q', length))
            buf_list.append(buf[i+56:i+56+length])

            i += 56 + length

        assert(i == len(buf)), 'invalid put request'

        if g.state['offset'] > g.max_file_size:
            raise Exception('size({0}) > maxsize({1})'.format(
                g.state['offset'], g.max_file_size))

        append(''.join(buf_list))
        return dict(buf=struct.pack('!B', 0) + 'ok')
    except:
        return dict(buf=struct.pack('!B', 1) + traceback.format_exc())


def append(buf):
    chksum = hashlib.sha1(g.state['checksum'].decode('hex'))
    chksum.update(buf)

    with open(os.path.join(g.data, str(g.state['filenum'])), 'ab', 0) as fd:
        fd.write(struct.pack('!Q', len(buf)) + buf + chksum.digest())

    g.state.update(scan(g.data, g.state['filenum'], g.state['offset'],
                        g.state['checksum'], index_put))


def get(src, buf):
    i = 0
    result = list()
    while i < len(buf):
        key = buf[i:i+32]
        f, o, l = index_get(key)
        result.append(key)
        result.append(struct.pack('!Q', f))
        result.append(struct.pack('!Q', o))
        result.append(struct.pack('!Q', l))
        if l > 0:
            with open(os.path.join(g.data, str(f)), 'rb') as fd:
                fd.seek(o)
                result.append(fd.read(l))
        i += 32

    return dict(buf=''.join(result))


def index_put(key, filenum, offset, length):
    g.sqlite.execute(
        'insert or replace into docs values(?, ?, ?, ?)',
        (sqlite3.Binary(key), filenum, offset, length))


def index_get(key):
    result = g.sqlite.execute(
        'select file, offset, length from docs where key=?',
        (sqlite3.Binary(key),)).fetchall()
    return result[0] if result else (0, 0, 0)


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

                    i = 0
                    while i < len(x):
                        key = x[i:i+32]
                        length = struct.unpack('!Q', x[i+32:i+40])[0]
                        ofst = offset+8+i+40
                        i += 40 + length

                        if callback:
                            callback(key, filenum, ofst, length)

                    offset += len(x) + 28
                    assert(offset <= total_size)
                    assert(offset == fd.tell())

                    result.update(dict(
                        filenum=filenum,
                        offset=offset,
                        size=total_size,
                        checksum=checksum.encode('hex'),
                        eof=True if(0 == len(x) and offset > 28) else False))

                    logging.critical(
                        ('scanned file({filenum}) offset({offset}) '
                         'size({size}) eof({eof})').format(**result))

            filenum += 1
            offset = 0
        except:
            return result


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
        g.state['filenum'], offset, length = result[0]
        with open(os.path.join(g.data, str(g.state['filenum'])), 'rb') as fd:
            fd.seek(offset + length)
            g.state['checksum'] = fd.read(20).encode('hex')
            g.state['offset'] = fd.tell()
            fd.seek(0, 2)
            g.state['size'] = fd.tell()
            assert(g.state['offset'] == offset + length + 20)
            assert(g.state['size'] >= g.state['offset'])

    files = sorted([int(f) for f in os.listdir(g.data)])
    if not files:
        return

    if '' == g.state['checksum']:
        with open(os.path.join(g.data, str(min(files))), 'rb') as fd:
            assert(0 == struct.unpack('!Q', fd.read(8))[0])
            g.state['filenum'] = min(files)
            g.state['checksum'] = fd.read(20).encode('hex')
            g.state['offset'] = fd.tell()
            assert(g.state['offset'] == 28)

    g.state.update(scan(g.data, g.state['filenum'], g.state['offset'],
                        g.state['checksum'], index_put))
    assert(max(files) == g.state['filenum'])

    f = os.open(os.path.join(g.data, str(g.state['filenum'])), os.O_RDWR)
    n = os.fstat(f).st_size
    if n > g.state['offset']:
        os.ftruncate(f, g.state['offset'])
        logging.critical('file({0}) truncated({1}) original({2})'.format(
            g.state['filenum'], g.state['offset'], n))
        g.state['size'] = g.state['offset']

    os.fsync(f)
    os.close(f)
    g.sqlite.commit()
