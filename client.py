import cmd
import time
import json
import msgio
import shlex
import struct
import pprint
import logging
import optparse

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
                time.sleep(1)
                self.server = None

        raise Exception('timed out')

    def state(self):
        return json.loads(self.request('state'))

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
            docs = dict()
            i = 1
            while i < len(buf):
                key_len = struct.unpack('!Q', buf[i:i+8])[0]
                key = buf[i+8:i+8+key_len]
                filenum = struct.unpack('!Q', buf[i+8+key_len:i+16+key_len])[0]
                offset = struct.unpack('!Q', buf[i+16+key_len:i+24+key_len])[0]
                docs[key] = (filenum, offset)
                i += key_len + 24

            return 0, docs

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


class Client(cmd.Cmd):
    prompt = '>'

    def __init__(self, servers):
        cmd.Cmd.__init__(self)
        self.cli = Lockr(servers)

    def do_EOF(self, line):
        self.do_quit(line)

    def do_quit(self, line):
        exit(0)

    def do_state(self, line):
        print(pprint.pformat(self.cli.state()).replace("u'", " '"))

    def do_get(self, line):
        for k, v in self.cli.get(line.split()).iteritems():
            print('{0} <{1}> {2}'.format(k, v[0], v[1]))

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
        docs = dict([(t[0], (t[1].strip('<>'), t[2])) for t in tup])
        code, value = self.cli.put(docs)
        if 0 == code:
            for k, v in value.iteritems():
                print('{0} <{1}-{2}>'.format(k, v[0], v[1]))
        else:
            print(value)


if '__main__' == __name__:
    parser = optparse.OptionParser()
    parser.add_option('--servers', dest='servers', type='string',
                      help='comma separated list of server:port')
    opt, args = parser.parse_args()

    logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

    Client(set(map(lambda x: (x[0], int(x[1])),
                   map(lambda x: x.split(':'),
                       opt.servers.split(','))))).cmdloop()
