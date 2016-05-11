import time
import json
import msgio
import struct

from logging import critical as log


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
        marker = (0, 0)
        sleep = 0
        while True:
            buf = self.request('watch', ''.join([struct.pack('!Q', marker[0]),
                                                 struct.pack('!Q', marker[1]),
                                                 key]))

            marker = (struct.unpack('!Q', buf[0:8])[0],
                      struct.unpack('!Q', buf[8:16])[0])

            keys = list()
            i = 16
            while i < len(buf):
                key_len = struct.unpack('!Q', buf[i:i+8])[0]
                keys.append(buf[i+8:i+8+key_len])

                i += 8 + key_len

            assert(i == len(buf))

            yield keys

            if keys:
                sleep = 0
            else:
                sleep = 1 if 0 == sleep else max(30, sleep*2)
                time.sleep(sleep)

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
