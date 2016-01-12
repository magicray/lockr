import os
import time
import json
import struct
import hashlib
import sqlite3
import traceback


class Globals():
    def __init__(self):
        self.candidate = None
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


def process_stats_request(peer, buf):
    msg = json.dumps(dict(
        file=g.filenum,
        offset=g.offset,
        netstats=g.stats))
    return [(peer, 'stats_response', msg)]


def process_stats_response(peer, buf):
    g.peers[peer] = json.loads(buf)
    return [(peer, 'stats_request', '')]


def process_replication_request(peer, buf):
    return [(peer, 259, '')]


def process_replication_response(peer, buf):
    return [(peer, 258, '')]


def process_leader_request(peer, buf):
    return [(peer, 261, '')]


def process_leader_response(peer, buf):
    return [(peer, 260, '')]


def process_lockr_state_request(peer, buf):
    state = dict()
    for ip, port in g.peers:
        state['{0}:{1}'.format(ip, port)] = g.peers[(ip, port)]

    return [(peer, '', json.dumps(dict(
        file=g.filenum,
        offset=g.offset,
        candidate=g.candidate,
        netstats=g.stats,
        peers=state,
        timestamp=time.strftime('%y%m%d.%H%M%S', time.gmtime()))))]


def process_lockr_put_request(peer, buf):
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

    return [(peer, '', result)]


def process_lockr_get_request(peer, buf):
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

    return [(peer, '', ''.join(result))]


def on_stats(stats):
    g.stats = stats


def on_connect(peer):
    return [(peer, 'stats_request', '')]


def on_disconnect(peer):
    pass


def on_accept(peer):
    pass


def on_reject(peer):
    pass


def on_init(port, servers, conf_file):
    if not os.path.isdir(g.data):
        os.mkdir(g.data)

    g.peers = dict((ip_port, None) for ip_port in servers)

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

        g.filenum, g.offset, g.checksum, g.total_size, _ = scan(g.data,
            g.filenum, g.offset, g.checksum, max(files), 2**50, put)
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
    g.sqlite.execute('insert or replace into docs values(?, ?, ?, ?)',
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


class StatsServer():
    def on_recv(self, buf):
        if 'stats' == buf:
            pass
        else:
            filenum, offset = buf.split('-')
            if os.path.isfile(os.path.join(C['data'], filenum)):
                with open(os.path.join(C['data'], filenum)) as fd:
                    fd.seek(int(offset))
                    return ('send', fd.read(10**6))
            else:
                return ('send', '')


class StatsClient():
    def on_recv(self, buf):
        if 'stats' == self.expected:
            stats = json.loads(buf)
            g_peers[self.peer] = stats

            local =g_db.file*2**64 + g_db.offset
            peer = stats['file']*2**64 + stats['offset']

            if peer > local:
                self.expected = '{0}-{1}'.format(
                    g_db.file, g_db.total_size)
                return ('send', self.expected)

            return ('send', self.expected)
        else:
            if len(buf):
                if not g_fd:
                    g_fd = os.open(
                        os.path.join('data', str(g_db.file)),
                        os.O_CREAT | os.O_WRONLY | os.O_APPEND)

                os.write(g_fd, buf)
                filenum, offset, chksum, total_size, file_closed = scan(
                    C['data'], g_db.file, g_db.offset,
                    g_db.checksum, g_db.file,
                    2**50, g_db.put)

                g_db.checksum = chksum
                if not file_closed:
                    g_db.offset = offset
                    g_db.total_size = total_size
                else:
                    os.close(g_fd)
                    g_fd = None
                    g_db.offset = 0
                    g_db.total_size = 0
                    g_db.file += 1

                self.expected = '{0}-{1}'.format(
                    g_db.file, g_db.total_size)
                return ('send', self.expected)
            else:
                self.expected = 'stats'
                return ('send', self.expected)
