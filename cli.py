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

    def get(self, line, with_values):
        begin, end = self.parse_line(line)
        offset, result = self.cli.get(begin, end, with_values)
        for k in sorted(result.keys()):
            print('{0} - {1}'.format(k, result[k]))

    def do_keys(self, line):
        self.get(line, False)

    def do_get(self, line):
        self.get(line, True)

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
        docs = dict([(t[0], (t[1], t[2])) for t in tup])
        code, value = self.cli.put(docs)
        if 0 == code:
            print('committed : {0}'.format(value))
        elif 1 == code:
            print('compare failed for following keys:-')
            for k, v in value.iteritems():
                print('{0} - {1}'.format(k, v))
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
            for i in range(10000):
                key = '%05d' % (i)
                offset, result = self.cli.get(key, key, True)
                prev = result.get(key, '')
                new = str(int(prev) + 1) if prev else '1'
                self.cli.put({key : (prev, new)})
