import cmd
import shlex
import client
import pprint
import struct


class Client(cmd.Cmd):
    prompt = '>'

    def __init__(self, servers):
        cmd.Cmd.__init__(self)
        self.cli = client.Lockr(servers)

    def do_EOF(self, line):
        self.do_quit(line)

    def do_quit(self, line):
        exit(0)

    def do_state(self, line):
        print(pprint.pformat(self.cli.state()).replace("u'", " '"))

    def parse_line(self, line):
        begin, end = '', ''
        if line:
            l = shlex.split(line)
            if 1 == len(l):
                begin = l[0]
                end = begin + struct.pack('!B', 255)
            else:
                begin, end = l[0], l[1]
        return begin, end

    def get(self, line, values_from):
        begin, end = self.parse_line(line)
        offset, result = self.cli.get(begin, end, values_from)
        for k in sorted(result.keys()):
            print('{0} <{1}> {2}'.format(k, result[k][0], result[k][1]))

    def do_keys(self, line):
        self.get(line, (2**64-1, 2**64-1))

    def do_get(self, line):
        self.get(line, (0, 0))

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
        docs = dict([(t[0], (int(t[1]), t[2])) for t in tup])
        code, value = self.cli.put(docs)
        if 0 == code:
            print('committed : {0}'.format(value))
        elif 1 == code:
            print('version mismatch for the following keys:-')
            for k, v in value.iteritems():
                print('{0} <{1}>'.format(k, v))
        else:
            print(value)

    def do_watch(self, line):
        begin, end = self.parse_line(line)
        while True:
            try:
                for result in self.cli.watch(begin, end):
                    for k in sorted(result['added'].keys()):
                        print('ADD {0} - {1}'.format(k, result['added'][k]))
                    for k in sorted(result['updated'].keys()):
                        print('MOD {0} - {1}'.format(k, result['updated'][k]))
                    for k in sorted(result['deleted']):
                        print('DEL {0}'.format(k))
            except:
                pass

    def do_test(self, line):
        while True:
            for i in range(100000):
                while True:
                    key = '%05d' % (i)
                    offset, result = self.cli.get(key, key, (0, 0))
                    prev = result.get(key, (0, '0'))
                    new = str(int(prev[1]) + 1)
                    req  = {key: (prev[0], new)}
                    res = self.cli.put(req)
                    print((req, res))
                    if 0 == res[0]:
                        break
