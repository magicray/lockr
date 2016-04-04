import cmd
import shlex
import lockr
import msgio
import pprint
import logging
import optparse

class Client(cmd.Cmd):
    prompt = '>'

    def __init__(self, servers):
        cmd.Cmd.__init__(self)
        self.cli = lockr.Lockr(servers)

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
            print('<{0}-{1}>'.format(value[0], value[1]))
        else:
            print('{0}'.format(value))

    def do_watch(self, line):
        cmd = shlex.split(line)

        code, result = self.cli.watch(
            dict(map(lambda x: (x[0], tuple(map(int,
                                                x[1].strip('<>').split('-')))),
                     zip(cmd[0::2], cmd[1::2]))))
        if 0 == code:
            key, txn, value = result
            print('{0} <{1}-{2}> {3}'.format(key, txn[0], txn[1], value))
        else:
            print(result)


parser = optparse.OptionParser()
parser.add_option('--port', dest='port', type='int',
                  help='port number')
parser.add_option('--peers', dest='peers', type='string',
                  help='comma separated list of ip:port')
parser.add_option('--servers', dest='servers', type='string',
                  help='comma separated list of server:port')
parser.add_option('--data', dest='data', type='string',
                  help='data directory', default='data')
parser.add_option('--max', dest='max', type='int',
                  help='max file size', default='256')
parser.add_option('--timeout', dest='timeout', type='int',
                  help='timeout in seconds', default='30')
parser.add_option('--cert', dest='cert', type='string',
                  help='certificate file path', default='cert.pem')
parser.add_option('--log', dest='log', type=int,
                  help='logging level', default=logging.INFO)

opt, args = parser.parse_args()

logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

if opt.port:
    lockr.g.opt = opt
    msgio.loop(lockr)
else:
    Client(set(map(lambda x: (x[0], int(x[1])),
                   map(lambda x: x.split(':'),
                       opt.servers.split(','))))).cmdloop()
