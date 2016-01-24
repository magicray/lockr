import sys
import ssl
import cmd
import time
import json
import shlex
import socket
import struct
import hashlib
import logging
import optparse
import traceback


class Lockr(object):
    def __init__(self, servers, timeout=30):
        self.servers = servers
        self.sock = None
        self.timeout = timeout

    def sndrcv(self, sock, req, buf=''):
        def recv(length):
            pkt = list()
            while length > 0:
                pkt.append(sock.recv(length))
                if 0 == len(pkt[-1]):
                    raise Exception('connection closed')
                length -= len(pkt[-1])
            return ''.join(pkt)

        t = time.time()
        sock.sendall(hashlib.sha1(req).digest() + struct.pack('!I', len(buf)))
        if buf:
            sock.sendall(buf)
        result = recv(struct.unpack('!I', recv(24)[20:])[0])
        logging.critical('received response(%s) from %s in %.3f msec' % (
            req, sock.getpeername(), (time.time() - t)*1000))
        return result

    def connect(self):
        for ip, port in self.servers:
            try:
                t = time.time()
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock = ssl.wrap_socket(sock)
                sock.connect((ip, port))
                logging.critical(
                    'connection to %s:%d succeeded in %.03f msec' % (
                        ip, port, (time.time()-t)*1000))
                stats = json.loads(self.sndrcv(sock, 'lockr_state_request'))
                if 'self' == stats['self']['leader']:
                    logging.critical('connected to leader {0}'.format(
                        sock.getpeername()))
                    return sock
            except:
                logging.critical(
                    'connection to %s:%d failed in %.03f msec' % (
                        ip, port, (time.time()-t)*1000))
        raise Exception('could not connect to server')

    def request(self, req, buf):
        t = time.time()
        while time.time() < (t + self.timeout):
            try:
                if not self.sock:
                    self.sock = self.connect()
                return self.sndrcv(self.sock, req, buf)
            except:
                time.sleep(1)
                self.sock = None

        raise Exception('timed out')

    def get_state(self):
        return json.loads(self.request('lockr_state_request', ''))

    def put(self, docs):
        items = [struct.pack('!B', 1)]
        for k, v in docs.iteritems():
            ver = '0-0' if (v[0] is '-' or v[0] is None) else v[0]

            items.append(hashlib.sha256(k).digest())
            items.append(struct.pack('!Q', int(ver.split('-')[0])))
            items.append(struct.pack('!Q', int(ver.split('-')[1])))
            items.append(struct.pack('!Q', len(v[1])))
            items.append(v[1])

        return self.request('lockr_put_request', ''.join(items))

    def get(self, keys):
        items = [struct.pack('!B', 2)]
        hashdict = dict()
        for key in keys:
            h = hashlib.sha256(key).digest()
            items.append(h)
            hashdict[h] = key

        buf = self.request('lockr_get_request', ''.join(items))
        i = 0
        docs = dict()
        while i < len(buf):
            k = hashdict[buf[i:i+32]]
            f = struct.unpack('!Q', buf[i+32:i+40])[0]
            o = struct.unpack('!Q', buf[i+40:i+48])[0]
            l = struct.unpack('!Q', buf[i+48:i+56])[0]
            docs[k] = ('{0}-{1}'.format(f, o), buf[i+56:i+56+l])

            i += 56 + l

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
        print(json.dumps(self.cli.get_state(), indent=4, sort_keys=True))

    def do_get(self, line):
        for k, v in self.cli.get(line.split()).iteritems():
            print('{0} <{1}> {2}'.format(k, v[0], v[1]))

    def do_put(self, line):
        try:
            cmd = shlex.split(line)
            tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
            docs = dict([(t[0], (t[1], t[2])) for t in tup])
            print(struct.unpack('!B', self.cli.put(docs))[0])
        except:
            traceback.print_exc()


if '__main__' == __name__:
    parser = optparse.OptionParser()
    parser.add_option('-s', '--servers', dest='servers', type='string',
                      help='comma separated list of ip:port')
    opt, args = parser.parse_args()

    logging.basicConfig(
        level=logging.NOTSET,
        format='%(asctime)s: %(message)s')

    servers = set(map(lambda x: (socket.gethostbyname(x[0]), int(x[1])),
                      map(lambda x: x.split(':'),
                          opt.servers.split(','))))

    Client(servers).cmdloop()
