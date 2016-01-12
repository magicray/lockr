import os
import time
import json
import struct
import hashlib
import sqlite3
import traceback


G = dict()
C = dict()


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
        file=G['index'].file,
        offset=G['index'].offset,
        netstats=G['netstats']))
    return [(peer, 'stats_response', msg)]


def process_stats_response(peer, buf):
    G['peers'][peer] = json.loads(buf)
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
    for ip, port in G['peers']:
        state['{0}:{1}'.format(ip, port)] = G['peers'][(ip, port)]

    return [(peer, '', json.dumps(dict(
        file=G['index'].file,
        offset=G['index'].offset,
        candidate=G['candidate'],
        netstats=G['netstats'],
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

    if G['index'].offset > C['size']:
        if G['fd']:
            G['index'].append_record(dict())
            os.close(G['fd'])
        G['index'].file += 1
        G['index'].offset = 0
        G['fd'] = None

    if not G['fd']:
        G['fd'] = os.open(
            os.path.join('data', str(G['index'].file)),
            os.O_CREAT | os.O_WRONLY | os.O_APPEND)

        if 0 == G['index'].offset:
            G['index'].append_record(dict())

    try:
        G['index'].append_record(docs)
        result = struct.pack('!B', 0)
    except:
        result = struct.pack('!B', 1)

    return [(peer, '', result)]


def process_lockr_get_request(peer, buf):
    result = list()
    i = 1
    while i < len(buf):
        key = buf[i:i+32]
        f, o, l = G['index'].get(key)
        result.append(key)
        result.append(struct.pack('!Q', f))
        result.append(struct.pack('!Q', o))
        result.append(struct.pack('!Q', l))
        if l > 0:
            with open(os.path.join('data', str(f))) as fd:
                fd.seek(o)
                result.append(fd.read(l))
        i += 32

    return [(peer, '', ''.join(result))]


def on_stats(stats):
    G['netstats'] = stats


def on_connect(peer):
    return [(peer, 'stats_request', '')]


def on_disconnect(peer):
    pass


def on_accept(peer):
    pass


def on_reject(peer):
    pass


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
            G['peers'][self.peer] = stats

            local = G['index'].file*2**64 + G['index'].offset
            peer = stats['file']*2**64 + stats['offset']

            if peer > local:
                self.expected = '{0}-{1}'.format(
                    G['index'].file, G['index'].total_size)
                return ('send', self.expected)

            return ('send', self.expected)
        else:
            if len(buf):
                if not G['fd']:
                    G['fd'] = os.open(
                        os.path.join('data', str(G['index'].file)),
                        os.O_CREAT | os.O_WRONLY | os.O_APPEND)

                os.write(G['fd'], buf)
                filenum, offset, chksum, total_size, file_closed = scan(
                    C['data'], G['index'].file, G['index'].offset,
                    G['index'].checksum, G['index'].file,
                    2**50, G['index'].put)

                G['index'].checksum = chksum
                if not file_closed:
                    G['index'].offset = offset
                    G['index'].total_size = total_size
                else:
                    os.close(G['fd'])
                    G['fd'] = None
                    G['index'].offset = 0
                    G['index'].total_size = 0
                    G['index'].file += 1

                self.expected = '{0}-{1}'.format(
                    G['index'].file, G['index'].total_size)
                return ('send', self.expected)
            else:
                self.expected = 'stats'
                return ('send', self.expected)


class DB():
    def __init__(self, db_file, data_dir):
        self.index = sqlite3.connect(db_file)
        self.index.execute('''create table if not exists docs
            (key blob primary key, file int, offset int, length int)''')

        self.timestamp = int(time.time())
        self.file = 0
        self.offset = 0
        self.total_size = 0
        self.checksum = ''

        result = self.index.execute('''select file, offset, length
            from docs order by file desc, offset desc limit 1''').fetchall()
        if result:
            offset = result[0][1] + result[0][2]
            with open(os.path.join('data', str(result[0][0]))) as fd:
                fd.seek(offset)
                self.file = result[0][0]
                self.offset = offset+20
                self.checksum = fd.read(20)

        files = sorted([int(f) for f in os.listdir(data_dir)])
        if files:
            if '' == self.checksum:
                with open(os.path.join(data_dir, str(min(files)))) as fd:
                    assert(0 == struct.unpack('!Q', fd.read(8))[0])
                    self.file = min(files)
                    self.offset = 28
                    self.checksum = fd.read(20)

            self.file, self.offset, self.checksum, self.total_size, _ = scan(
                data_dir, self.file, self.offset, self.checksum, max(files),
                2**50, self.put)
            assert(max(files) == self.file)
            # with open(os.path.join(data_dir, str(self.file)), 'w') as fd:
            #     fd.truncate(self.offset)
            self.index.commit()

    def commit(self):
        if int(time.time()) > self.timestamp:
            self.index.commit()
            self.timestamp = int(time.time())

    def put(self, key, filenum, offset, length):
        self.commit()
        self.index.execute(
            'insert or replace into docs values(?, ?, ?, ?)',
            (sqlite3.Binary(key), filenum, offset, length))

    def get(self, key):
        self.commit()
        result = self.index.execute(
            'select file, offset, length from docs where key=?',
            (sqlite3.Binary(key),)).fetchall()
        return result[0] if result else (0, 0, 0)

    def append_record(self, docs):
        buf_len = 0
        buf_list = list()
        for k, v in docs.iteritems():
            f, o, l = self.get(k)
            assert((v[0] == f) and (v[1] == o)), 'version mismatch'

            buf_list.append(k)
            buf_list.append(struct.pack('!Q', len(v[2])))
            buf_list.append(v[2])
            buf_len += 40 + len(v[2])

        offset = self.offset
        offset += 8
        for k, v in docs.iteritems():
            self.put(k, self.file, offset+32+8, len(v[2]))
            offset += 32 + 8 + len(v[2])

        chksum = hashlib.sha1(self.checksum)
        map(lambda b: chksum.update(b), buf_list)

        buf_list.insert(0, struct.pack('!Q', buf_len))
        buf_list.append(chksum.digest())

        os.write(G['fd'], ''.join(buf_list))

        self.offset = offset + 20
        self.checksum = chksum.digest()


def on_init(config_file, port, servers):
    global G
    global C

    if not config_file:
        C = dict(index='index', data='data', size=100)

    if not os.path.isdir(C['data']):
        os.mkdir(C['data'])

    G = dict(netstats=dict(),
             fd=None,
             candidate=None,
             index=DB(C['index'], C['data']),
             peers=dict((ip_port, None) for ip_port in servers))