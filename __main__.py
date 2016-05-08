import sys
import msgio
import server
import client
import signal
import random
import logging
import optparse

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

opt, args = parser.parse_args()

logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

nodes = set(map(lambda x: (x.split(':')[0], int(x.split(':')[1])),
                opt.nodes.split(',')))

if opt.port:
    server.init(nodes, opt)
    signal.alarm(random.randint(opt.timeout, 2*opt.timeout))
    msgio.loop(server, opt.port, nodes)
else:
    client.Client(nodes).cmdloop()
