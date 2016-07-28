import time
import json
import msgio
import struct
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


class Lockr(object):
    def __init__(self, servers, timeout=10**8):
        self.servers = servers
        self.server = None
        self.timeout = timeout

    def request(self, req, buf=''):
        req_begin = time.time()
        backoff = 1
        while time.time() < req_begin + self.timeout:
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
                                break

                            s.close()
                        except:
                            logger.debug('connection to(%s) failed msec(%d)',
                                         srv, (time.time()-t)*1000)
                if self.server:
                    self.server.send(req, buf)
                    result = self.server.recv()
                    logger.critical('received response(%s) from%s msec(%d)',
                                    req, self.server.server,
                                    (time.time() - req_begin)*1000)
                    return result

                time.sleep(backoff)
                backoff = min(60, backoff*2)
            except:
                if self.server:
                    self.server.close()

                self.server = None

    def state(self):
        return json.loads(self.request('state'))

    def put(self, docs):
        items = list()
        for k, v in docs.iteritems():
            items.append(struct.pack('!Q', len(k)))
            items.append(k)
            items.append(struct.pack('!Q', v[0]))
            items.append(struct.pack('!Q', v[1]))
            items.append(struct.pack('!Q', len(v[2])))
            items.append(v[2])

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

    def get(self, keys, offset=(0, 0)):
        buf_list = [struct.pack('!Q', offset[0]),
                    struct.pack('!Q', offset[1])]

        for key, version in keys.iteritems():
            buf_list.append(struct.pack('!Q', len(key)))
            buf_list.append(key)
            buf_list.append(struct.pack('!Q', version))

        buf = self.request('get', ''.join(buf_list))

        code = struct.unpack('!B', buf[0])[0]
        if code > 1:
            raise Exception(buf[1:])

        offset = (struct.unpack('!Q', buf[1:9])[0],
                  struct.unpack('!Q', buf[9:17])[0])

        i = 17
        result = dict()
        while i < len(buf):
            key_len = struct.unpack('!Q', buf[i:i+8])[0]
            key = buf[i+8:i+8+key_len]
            ver = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
            if ver > 0:
                v_len = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
                result[key] = (ver, buf[i+24+key_len:i+24+key_len+v_len])
                i += 24 + key_len + v_len
            else:
                result[key] = (ver, None)
                i += 16 + key_len

        assert(i == len(buf))

        return code, offset, result

    def watch(self, watched):
        offset = (0, 0)
        keys = dict()

        while True:
            key_ver = dict([(k, keys.get(k, 0)) for k in watched])
            code, offset, ret = self.get(key_ver, (offset[0], offset[1]+1))
            result = dict(added=dict(), updated=dict(), deleted=set())

            for key, (version, value) in ret.iteritems():
                if value:
                    if key not in keys:
                        result['added'][key] = value
                        keys[key] = version
                    elif version > keys[key]:
                        result['updated'][key] = value
                        keys[key] = version
                else:
                    if key in keys:
                        result['deleted'].add(key)
                    keys.pop(key, None)

            if 0 == code:
                for k in set(keys)-set(result['added'])-set(result['updated']):
                    if k in keys:
                        result['deleted'].add(k)
                    keys.pop(k, None)

            if result['added'] or result['updated'] or result['deleted']:
                yield result
