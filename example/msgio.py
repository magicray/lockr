import random
import optparse


def on_connect(src):
    return dict(msg='ping', buf='')


def on_disconnect(src, exc, tb):
    print('ERROR({0})'.format(exc))
    print(tb)


def on_message(src, msg, buf):
    print('from({0}) {1}({2})'.format(src, msg, len(buf)))

    return dict(msg='ping', buf=' '*random.randrange(1, 1024*1024))


def on_init():
    parser = optparse.OptionParser()
    parser.add_option('--node', dest='node', type='string',
                      help='port')
    parser.add_option('--port', dest='port', type='int',
                      help='port')
    parser.add_option('--peers', dest='peers', type='string',
                      help='comma separated list of ip:port')
    opt, args = parser.parse_args()

    return dict(port=opt.port,
                node=opt.node,
                peers=set(opt.peers.split(',')))
