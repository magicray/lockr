import msgio
import lockr
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
parser.add_option('--maxsize', dest='max_size', type='int',
                  help='max file size', default='256')
parser.add_option('--timeout', dest='timeout', type='int',
                  help='timeout in seconds', default='30')

opt, args = parser.parse_args()

logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

nodes = set(map(lambda x: (x.split(':')[0], int(x.split(':')[1])),
                opt.nodes.split(',')))

if opt.port:
    lockr.init(nodes, opt.data, opt.max_size)
    signal.alarm(random.randint(opt.timeout, 2*opt.timeout))
    msgio.loop(lockr, opt.port, nodes)
else:
    client.Client(nodes).cmdloop()
