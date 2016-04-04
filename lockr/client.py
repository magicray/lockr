from __future__ import absolute_import
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
                            stats = json.loads(msgio.request(srv, 'state'))
                            log('connection to %s succeeded in %.03f msec' % (
                                srv, (time.time()-t)*1000))
                            if 'leader' == stats['state']:
                                self.server = srv
                                log('connected to leader {0}'.format(srv))
                                break
                        except:
                            log('connection to %s failed in %.03f msec' % (
                                srv, (time.time()-t)*1000))

                result = msgio.request(self.server, req, buf)
                log('received response(%s) from %s in %0.3f msec' % (
                    req, self.server, (time.time() - req_begin)*1000))
                return result
            except:
                self.server = None

        raise Exception('timed out')

    def state(self):
        return json.loads(self.request('state'))

    def watch(self, docs):
        buf = list()
        for k, v in docs.iteritems():
            buf.append(struct.pack('!Q', len(k)))
            buf.append(k)
            buf.append(struct.pack('!Q', v[0]))
            buf.append(struct.pack('!Q', v[1]))

        buf = self.request('watch', ''.join(buf))

        if 0 == struct.unpack('!B', buf[0])[0]:
            key_len = struct.unpack('!Q', buf[1:9])[0]
            key = buf[9:9+key_len]
            txn = (struct.unpack('!Q', buf[9+key_len:17+key_len])[0],
                   struct.unpack('!Q', buf[17+key_len:25+key_len])[0])
            value_len = struct.unpack('!Q', buf[25+key_len:33+key_len])[0]
            value = buf[33+key_len:33+key_len+value_len]

            return (0, (key, txn, value))
        else:
            return struct.unpack('!B', buf[0])[0], buf[1:]

    def put(self, docs):
        items = list()
        for k, v in docs.iteritems():
            ver = '0-0' if (v[0] is '-' or v[0] is None) else v[0]

            items.append(struct.pack('!Q', len(k)))
            items.append(k)
            items.append(struct.pack('!Q', int(ver.split('-')[0])))
            items.append(struct.pack('!Q', int(ver.split('-')[1])))
            items.append(struct.pack('!Q', len(v[1])))
            items.append(v[1])

        buf = self.request('put', ''.join(items))
        if 0 == struct.unpack('!B', buf[0])[0]:
            f = struct.unpack('!Q', buf[1:9])[0]
            o = struct.unpack('!Q', buf[9:17])[0]

            return 0, (f, o)

        return struct.unpack('!B', buf[0])[0], buf[1:]

    def get(self, keys):
        buf = list()
        for key in keys:
            buf.append(struct.pack('!Q', len(key)))
            buf.append(key)

        buf = self.request('get', ''.join(buf))

        i = 0
        docs = dict()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            f = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            o = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
            value_len = struct.unpack('!Q', buf[i+24+key_len:i+32+key_len])[0]
            value = buf[i+32+key_len:i+32+key_len+value_len]

            docs[key] = ('{0}-{1}'.format(f, o), value)

            i += 32 + key_len + value_len

        return docs
