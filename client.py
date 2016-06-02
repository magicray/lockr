import time
import json
import msgio
import struct
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


class Lockr(object):
    def __init__(self, servers, timeout=5):
        self.servers = servers
        self.server = None
        self.timeout = timeout

        self.offset = (0, 0)
        self.keys = set()

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
                            logger.debug('connected to(%s) msec(%d)',
                                         srv, (time.time()-t)*1000)
                            if 'leader' == stats['state']:
                                self.server = s
                                logger.debug('connected to leader(%s)',
                                             str(srv))
                                break

                            s.close()
                        except:
                            logger.debug('connection to(%s) failed msec(%d)',
                                         srv, (time.time()-t)*1000)

                self.server.send(req, buf)
                result = self.server.recv()
                logger.critical('received response(%s) from%s msec(%d)',
                                req, self.server.server,
                                (time.time() - req_begin)*1000)
                return result
            except:
                self.server = None

        raise Exception('timed out {0}'.format(time.time()-req_begin))

    def state(self):
        return json.loads(self.request('state'))

    def put(self, docs):
        items = list()
        for k, v in docs.iteritems():
            items.append(struct.pack('!Q', len(k)))
            items.append(k)
            items.append(struct.pack('!Q', v[0]))
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
                kv[key] = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]

                i += 16 + key_len

            return 1, kv

        return struct.unpack('!B', buf[0])[0], buf[1:]

    def get(self, begin, end, flags=False, offset=(0, 0)):
        buf = self.request('get', ''.join([
            struct.pack('!Q', offset[0]),
            struct.pack('!Q', offset[1]),
            struct.pack('!I', flags),
            struct.pack('!Q', len(begin)),
            begin,
            struct.pack('!Q', len(end)),
            end]))

        if 0 != struct.unpack('!B', buf[0])[0]:
            raise Exception(buf[1:])

        offset = (struct.unpack('!Q', buf[1:9])[0],
                  struct.unpack('!Q', buf[9:17])[0])

        i = 17
        result = dict()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            ver = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            val_len = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]

            if flags:
                val = buf[i+24+key_len:i+24+key_len+val_len]
                result[key] = dict(version=ver, value=val, length=val_len)
                i += 24 + key_len + val_len
            else:
                result[key] = dict(version=ver, length=val_len)
                i += 24 + key_len

        assert(i == len(buf))

        return offset, result

    def watch(self, begin, end):
        while True:
            self.offset, ret = self.get(begin, end, True, self.offset)
            result = dict(added=dict(), updated=dict())
            for key, value in ret.iteritems():
                if value['value']:
                    if key in self.keys:
                        result['updated'][key] = value['value']
                    else:
                        result['added'][key] = value['value']

            result['deleted'] = self.keys - set(ret)
            self.keys = set(ret)

            if result['added'] or result['updated'] or result['deleted']:
                yield result

            self.request('watch', ''.join([
                struct.pack('!Q', self.offset[0]),
                struct.pack('!Q', self.offset[1])]))
