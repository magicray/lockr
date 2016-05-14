import os
import time
import fcntl
import signal
import random
import logging
import optparse


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

    logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

    nodes = set(map(lambda x: (x.split(':')[0], int(x.split(':')[1])),
                    opt.nodes.split(',')))

    if opt.port:
        fcntl.flock(os.open('.', os.O_RDONLY), fcntl.LOCK_EX | fcntl.LOCK_NB)

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
        import cli
        cli.Client(nodes).cmdloop()
