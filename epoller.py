import os
import ssl
import time
import socket
import select
import struct
import hashlib
import inspect
import logging
import optparse
import traceback


def epoll_loop(module, port, clients):
    callbacks = dict()
    for m in inspect.getmembers(module, inspect.isfunction):
        if m[0].startswith('callback_'):
            callbacks[hashlib.sha1(m[0][9:]).digest()] = (
                getattr(module, m[0]), m[0][9:])
            logging.info('registered {0}'.format(m[0]))

    listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener_sock.bind(port)
    listener_sock.listen(5)
    listener_sock.setblocking(0)
    logging.critical('listening on {0}'.format(port))

    epoll = select.epoll()
    epoll.register(listener_sock.fileno(), select.EPOLLIN)

    stats = dict(in_pkt=0, in_bytes=0, out_pkt=0, out_bytes=0,
                 srv_accept=0, srv_established=0, srv_disconnect=0,
                 cli_connect=0, cli_established=0, cli_disconnect=0)

    connections = dict()
    addr2fd = dict()
    clients = set(clients)

    while True:
        while clients:
            ip_port = clients.pop()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.setblocking(0)
            connections[s.fileno()] = dict(
                sock=s,
                handshake_done=False,
                is_server=False,
                ip_port=ip_port)
            epoll.register(s.fileno(), select.EPOLLOUT)
            try:
                stats['cli_connect'] += 1
                logging.debug('connect initiated to {0}'.format(ip_port))
                s.connect(ip_port)
            except Exception as e:
                if 115 != e.errno:
                    raise

        for fileno, event in epoll.poll():
            if listener_sock.fileno() == fileno:
                s, addr = listener_sock.accept()
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                stats['srv_accept'] += 1
                s.setblocking(0)
                s = ssl.wrap_socket(s,
                                    certfile=opt.cert,
                                    do_handshake_on_connect=False,
                                    server_side=True)
                connections[s.fileno()] = dict(
                    sock=s,
                    is_server=True,
                    handshake_done=False)
                epoll.register(s.fileno(), select.EPOLLIN)
                continue

            try:
                conn = connections[fileno]
                conn['ssl_error'] = None
                out_msg_list = None

                if conn['handshake_done'] and event & select.EPOLLIN:
                    buf = conn['sock'].recv(conn['in_size'])
                    stats['in_bytes'] += len(buf)
                    assert(len(buf) > 0)

                    conn['in_pkts'].append(buf)
                    conn['in_size'] -= len(buf)

                    if 0 == conn['in_size']:
                        buf = ''.join(conn['in_pkts'])
                        if 'name' not in conn:
                            name = buf[0:20]
                            size = struct.unpack('!I', buf[20:])[0]
                            assert(name in callbacks), 'closed by peer'

                            conn['name'] = name
                            if 0 == size:
                                conn['buf'] = ''
                            else:
                                conn['in_size'] = size
                                conn['in_pkts'] = list()
                        else:
                            conn['buf'] = buf

                    if 'buf' in conn:
                        method, name = callbacks[conn['name']]
                        logging.debug('peer{0} callback({1}) size({2})'.format(
                            conn['peer'], name, len(conn['buf'])))
                        out_msg_list = method(conn['peer'], conn['buf'])

                        stats['in_pkt'] += 1
                        conn['in_size'] = 24
                        conn['in_pkts'] = list()
                        del(conn['name'])
                        del(conn['buf'])

                if conn['handshake_done'] and event & select.EPOLLOUT:
                    if 'pkt' not in conn:
                        if conn['msgs']:
                            conn['pkt'] = conn['msgs'].pop(0)
                            conn['sent'] = 0

                    if 'pkt' in conn:
                        if len(conn['pkt']) > conn['sent']:
                            n = conn['sock'].send(conn['pkt'][
                                    conn['sent']:conn['sent']+8*1024])

                            conn['sent'] += n
                            stats['out_bytes'] += n

                        if len(conn['pkt']) == conn['sent']:
                            del(conn['pkt'])
                            del(conn['sent'])
                            if(0 == len(conn['msgs'])%2):
                                stats['out_pkt'] += 1

                if False == conn['handshake_done']:
                    if getattr(conn['sock'], 'do_handshake', None) is None:
                        conn['sock'] = ssl.wrap_socket(
                            conn['sock'],
                            do_handshake_on_connect=False,
                            server_side=False)

                    conn['sock'].do_handshake()
                    conn['handshake_done'] = True
                    conn['peer'] = conn['sock'].getpeername()
                    conn['in_size'] = 24
                    conn['in_pkts'] = list()
                    conn['msgs'] = list()
                    addr2fd[conn['peer']] = fileno
                    name = 'on_accept' if conn['is_server'] else 'on_connect'

                    logging.info('peer({0}) callback({1})'.format(
                            conn['peer'], name))

                    out_msg_list = getattr(module, name)(conn['peer'])

                if event & ~(select.EPOLLIN | select.EPOLLOUT):
                    raise Exception('unhandled event({0})'.format(event))

            except ssl.SSLError as e:
                peer = conn['sock'].getpeername()
                if ssl.SSL_ERROR_WANT_READ == e.errno:
                    conn['ssl_error'] = select.EPOLLIN
                    logging.debug('peer{0} ssl(want_read)'.format(peer))
                elif ssl.SSL_ERROR_WANT_WRITE == e.errno:
                    conn['ssl_error'] = select.EPOLLOUT
                    logging.debug('peer{0} ssl(want_write)'.format(peer))
                else:
                    logging.error('peer{0} ssl({1})'.format(peer, e.errno))
                    traceback.print_exc()
                    exit(1)
            except Exception as e:
                conn['close'] = str(e)
            finally:
                if 'close' in conn:
                    conn = connections.pop(fileno)

                    conn['sock'].close()
                    epoll.unregister(fileno)

                    addr2fd.pop(conn.get('peer'), None)

                    if conn['is_server']:
                        if conn['handshake_done']:
                            out_msg_list = getattr(module, 'on_reject')(
                                conn['peer'])
                            logging.info('peer({0}) callback({1})'.format(
                                conn['peer'], 'on_reject'))
                        stats['srv_disconnect'] += 1
                    else:
                        if conn['handshake_done']:
                            out_msg_list = getattr(module, 'on_disconnect')(
                                conn['ip_port'])
                            logging.info('peer({0}) callback({1})'.format(
                                conn['ip_port'], 'on_disconnect'))
                        stats['cli_disconnect'] += 1
                        clients.add((conn['ip_port'][0], conn['ip_port'][1]))

                if out_msg_list:
                    if type(out_msg_list) is dict:
                        out_msg_list = [out_msg_list]

                    for d in out_msg_list:
                        dst = d.get('dst', conn['peer'])
                        msg_type = d.get('type', 'default')
                        buf = d.get('buf', '')

                        if dst in addr2fd:
                            f = addr2fd[dst]
                            connections[f]['msgs'].append(''.join([
                                hashlib.sha1(msg_type).digest(),
                                struct.pack('!I', len(buf))]))
                            connections[f]['msgs'].append(buf)
                            epoll.modify(
                                f,
                                select.EPOLLIN | select.EPOLLOUT)
                        else:
                            logging.critical('{0} not found'.format(dst))

                if 'close' not in conn:
                    if conn['ssl_error']:
                        epoll.modify(fileno, conn['ssl_error'])
                    elif conn['msgs'] or ('pkt' in conn):
                        epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT)
                    else:
                        epoll.modify(fileno, select.EPOLLIN)

        getattr(module, 'on_stats')(stats)


if '__main__' == __name__:
    parser = optparse.OptionParser()
    parser.add_option('-b', '--bind', dest='port', type='string',
                      help='server:port tuple. skip to start the client')
    parser.add_option('-s', '--servers', dest='servers', type='string',
                      help='comma separated list of ip:port')
    parser.add_option('-m', '--module_name', dest='module', type='string',
                      help='module name')
    parser.add_option('-f', '--file', dest='cert', type='string',
                      help='certificate file path', default='cert.pem')
    parser.add_option('-c', '--conf', dest='conf', type='string',
                      help='configuration option')
    parser.add_option('-l', '--log', dest='log', type='string',
                      help='logging level', default='warning')
    opt, args = parser.parse_args()

    time.sleep(1)

    logging.basicConfig(
        format='%(asctime)s: %(message)s',
        level={
            'critical': logging.CRITICAL,
            'error': logging.ERROR,
            'warning': logging.WARNING,
            'info': logging.INFO,
            'debug': logging.DEBUG,
            'notset': logging.NOTSET}[opt.log])

    if not os.path.isfile(opt.cert):
        os.mknod(opt.cert)
        os.system('openssl req -new -x509 -days 365 -nodes -newkey rsa:2048 '
              ' -subj "/" -out {0} -keyout {0} 2> /dev/null'.format(opt.cert))

    servers = list()
    if opt.servers:
        servers = set(map(lambda x: (socket.gethostbyname(x[0]), int(x[1])),
                          map(lambda x: x.split(':'),
                              opt.servers.split(','))))
    port = (socket.gethostbyname(opt.port.split(':')[0]),
            int(opt.port.split(':')[1]))

    module = __import__(
        opt.module,
        fromlist='.'.join(opt.module.split('.')[:-1]))
    module.on_init(port, servers, opt.conf)
    epoll_loop(module, port, servers)
