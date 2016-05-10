import os
import cmd
import time
import shlex
import client
import pprint
import signal
import random
import logging
import optparse


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
                for value in self.cli.watch(key):
                    print(value)
            except:
                pass


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='int',
                      help='port number')
    parser.add_option('--nodes', dest='nodes', type='string',
                      help='comma separated list of server:port')
    parser.add_option('--data', dest='data', type='string',
                      help='data directory', default='data')
    parser.add_option('--index', dest='index', type='string',
                      help='index file path', default='snapshot.db')
    parser.add_option('--maxsize', dest='max_size', type='int',
                      help='max file size', default='256')
    parser.add_option('--replsize', dest='repl_size', type='int',
                      help='replication chunk size', default=10*2**20)
    parser.add_option('--timeout', dest='timeout', type='int',
                      help='timeout in seconds', default='20')
    parser.add_option('--logfile', dest='logfile', type='str',
                      help='logfile name')

    opt, args = parser.parse_args()

    nodes = set(map(lambda x: (x.split(':')[0], int(x.split(':')[1])),
                    opt.nodes.split(',')))

    if opt.port:
        os.close(0)
        os.close(1)

        if opt.logfile:
            if os.fork():
                os._exit(0)

            os.setsid()

        while True:
            if 0 == os.fork():
                import msgio
                import server

                logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

                if opt.logfile:
                    os.dup2(os.open('{0}.{1}'.format(
                        opt.logfile,
                        time.strftime('%y%m%d', time.gmtime())),
                        os.O_CREAT | os.O_WRONLY | os.O_APPEND), 2)

                logging.critical('')
                server.init(nodes, opt)
                signal.alarm(random.randint(opt.timeout, 2*opt.timeout))
                msgio.loop(server, opt.port, nodes)
                os._exit(0)

            os.wait()
    else:
        Client(nodes).cmdloop()
