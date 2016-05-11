import cmd
import shlex
import client
import pprint


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

    def do_keys(self, line):
        prefix = shlex.split(line)[0] if line else ''
        for k in self.cli.keys(prefix):
            print(k)

    def do_get(self, line):
        result = self.cli.get(shlex.split(line))
        for k in sorted(result.keys()):
            print('{0} - {1}'.format(k, result[k]))

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
        key = shlex.split(line)[0]
        while True:
            try:
                for result in self.cli.watch(key):
                    kv = self.cli.get(result)
                    for k in sorted(kv.keys()):
                        print('{0} - {1}'.format(k, kv[k]))
            except:
                pass
