import lockr
import msgio
import client
import logging
import optparse


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
    client.Client(set(map(lambda x: (x[0], int(x[1])),
                      map(lambda x: x.split(':'),
                          opt.servers.split(','))))).cmdloop()
