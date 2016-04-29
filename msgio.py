import os
import ssl
import time
import copy
import socket
import select
import struct
import marshal
import logging
import traceback
import collections


logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


def loop(module, port, peers):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(0)
        s.connect(('0.0.0.0', port))
    except:
        port = (s.getsockname()[0], port)

    cert = os.getenv('MSGIO_CERT', 'cert.pem')
    node = os.getenv('MSGIO_NODE', '{0}:{1}'.format(*port))
    key = os.getenv('MSGIO_KEY', '')
    os.environ['MSGIO_NODE'] = node

    clients = set(filter(lambda x: x > port,
                         map(lambda x: (socket.gethostbyname(x[0]), x[1]),
                             peers)))

    listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener_sock.setblocking(0)
    listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener_sock.bind(port)
    listener_sock.listen(5)
    logger.critical('listening on %s', port)

    epoll = select.epoll()
    epoll.register(listener_sock.fileno(), select.EPOLLIN)

    connections = dict()
    addr2fd = dict()

    last_stats_time = time.time()
    last_connect_time = 0

    stats = dict(in_pkts=0, in_bytes=0, out_pkts=0, out_bytes=0, dropped=0,
                 srv_accept=0, srv_established=0, srv_disconnect=0,
                 cli_connect=0, cli_established=0, cli_disconnect=0)

    if not os.path.isfile(cert):
        os.mknod(cert)
        os.system('openssl req -new -x509 -days 365 -nodes -newkey rsa:2048 '
                  '-subj "/" -out {0} -keyout {0} 2> /dev/null'.format(cert))

    while True:
        if time.time() > last_connect_time + 1:
            last_connect_time = time.time()

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
                    logger.debug('connecting to %s', ip_port)
                    s.connect(ip_port)
                except Exception as e:
                    if 115 != e.errno:
                        raise

        for fileno, event in epoll.poll(1):
            if listener_sock.fileno() == fileno:
                s, addr = listener_sock.accept()
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                stats['srv_accept'] += 1
                s.setblocking(0)
                s = ssl.wrap_socket(s,
                                    certfile=cert,
                                    do_handshake_on_connect=False,
                                    server_side=True)
                connections[s.fileno()] = dict(
                    sock=s,
                    is_server=True,
                    handshake_done=False)
                epoll.register(s.fileno(), select.EPOLLIN)
                logger.debug('accepted connection from %s', addr)
                continue

            try:
                conn = connections[fileno]
                out_msg_list = list()

                if conn['handshake_done'] and event & select.EPOLLIN:
                    conn['in'].append(conn['sock'].recv(32*1024))
                    stats['in_bytes'] += len(conn['in'][-1])
                    assert(len(conn['in'][-1]) > 0)

                    size = struct.unpack('!Q', conn['in'][0][0:8])[0] + 10

                    total_size = sum(map(len, conn['in']))
                    assert(total_size <= size)

                    if size == total_size:
                        msg_len = struct.unpack(
                            '!H', conn['in'][0][8:10])[0]
                        msg = conn['in'][0][10:10+msg_len]

                        conn['in'][0] = conn['in'][0][10+msg_len:]
                        buf = ''.join(conn['in'])

                        if '' == msg:
                            cred = marshal.loads(buf)
                            assert(key == cred['key'])
                            conn['src'] = cred['node']
                            addr2fd[conn['src']] = fileno
                            logger.debug('from%s node(%s) msg(%s) len(%d)',
                                conn['peer'], conn['src'], msg, len(buf))

                            if conn['is_server'] is True:
                                out_msg_list.append(dict(
                                    msg='',
                                    buf=marshal.dumps(dict(
                                        node=node, key=key))))
                        try:
                            if '' == msg:
                                out_msg_list.append(module.on_connect(
                                    conn['src']))
                            else:
                                src = conn.get('src', conn['peer'])
                                logger.info('from%s node(%s) msg(%s) len(%d)',
                                    conn['peer'], src, msg, len(buf))
                                out_msg_list.append(module.on_message(
                                    src, msg, buf))
                        except Exception as e:
                            conn['close'] = (e, traceback.format_exc())
                            raise

                        stats['in_pkts'] += 1
                        conn['in'] = list()

                if conn['handshake_done'] and event & select.EPOLLOUT:
                    if conn['out'][0]:
                        n = conn['sock'].send(conn['out'][0][1][:32*1024])
                        conn['out'][0][1] = conn['out'][0][1][n:]
                        stats['out_bytes'] += n

                    if not conn['out'][0][1]:
                        src = conn.get('src', conn['peer'])
                        meta, _ = conn['out'].popleft()
                        logger.info('to%s node(%s) msg(%s) len(%d)',
                            conn['peer'], src, meta[0], meta[1])
                        stats['out_pkts'] += 1

                if False == conn['handshake_done']:
                    if getattr(conn['sock'], 'do_handshake', None) is None:
                        conn['sock'] = ssl.wrap_socket(
                            conn['sock'],
                            do_handshake_on_connect=False,
                            server_side=False)

                    conn['sock'].do_handshake()
                    conn['handshake_done'] = True
                    conn['peer'] = conn['sock'].getpeername()
                    conn['in'] = list()
                    conn['out'] = collections.deque()
                    addr2fd[conn['peer']] = fileno
                    logger.debug('ssl handshake done for %s', conn['peer'])

                    if conn['is_server'] is False:
                        stats['cli_established'] += 1
                        out_msg_list.append(dict(msg='', buf=marshal.dumps(
                            dict(node=node, key=key))))
                    else:
                        stats['srv_established'] += 1

                if event & ~(select.EPOLLIN | select.EPOLLOUT):
                    logger.error('unhandled event(%0x)', event)
                    raise Exception('unhandled event({0})'.format(event))

            except ssl.SSLError as e:
                if ssl.SSL_ERROR_WANT_READ == e.errno:
                    epoll.modify(fileno, select.EPOLLIN)
                elif ssl.SSL_ERROR_WANT_WRITE == e.errno:
                    epoll.modify(fileno, select.EPOLLOUT)
                else:
                    conn['close'] = (None, traceback.format_exc())
            except Exception as e:
                if 'close' not in conn:
                    conn['close'] = (None, traceback.format_exc())
            finally:
                if 'close' in conn:
                    exc, tb = conn['close']

                    conn = connections.pop(fileno)

                    conn['sock'].close()
                    epoll.unregister(fileno)

                    addr2fd.pop(conn.get('peer'), None)
                    addr2fd.pop(conn.get('src'), None)

                    if conn['handshake_done'] is True and 'src' in conn:
                        logger.info('disconnected%s node(%s)',
                            conn['peer'], conn['src'])
                        out_msg_list.append(module.on_disconnect(
                            conn['src'], exc, tb))

                    if conn['is_server']:
                        stats['srv_disconnect'] += 1
                        logger.debug('disconnected%s', conn['peer'])
                    else:
                        stats['cli_disconnect'] += 1
                        logger.debug('disconnected%s', conn['ip_port'])
                        clients.add(conn['ip_port'])

                if out_msg_list:
                    for l in filter(lambda x: x is not None, out_msg_list):
                        for d in l if type(l) is list else [l]:
                            dst = d.get('dst', conn['peer'])
                            msg = d.get('msg', '')
                            buf = d.get('buf', '')

                            if dst not in addr2fd:
                                stats['dropped'] += 1
                                continue

                            f = addr2fd[dst]
                            connections[f]['out'].append([
                                (msg, len(buf)),
                                ''.join([
                                    struct.pack('!Q', len(msg) + len(buf)),
                                    struct.pack('!H', len(msg)),
                                    msg,
                                    buf])])
                            epoll.modify(f, select.EPOLLIN | select.EPOLLOUT)

                if conn['handshake_done'] is True and 'close' not in conn:
                    if conn['out']:
                        epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT)
                    else:
                        epoll.modify(fileno, select.EPOLLIN)

        if time.time() > last_stats_time + 10:
            duration = int(time.time() - last_stats_time)
            last_stats_time = time.time()

            logger.critical(
                'sec(%d) bytes(%d, %d) pkts(%d, %d, %d) conns(%d) '
                'cli(%d, %d, %d) srv(%d, %d, %d)',
                duration,
                stats['in_bytes'], stats['out_bytes'],
                stats['in_pkts'], stats['out_pkts'], stats['dropped'],
                len(connections),
                stats['cli_connect'], stats['cli_disconnect'],
                stats['cli_established'],
                stats['srv_accept'], stats['srv_disconnect'],
                stats['srv_established'])

            for k in stats:
                stats[k] = 0


class Client(object):
    def __init__(self, server):
        self.server = server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock = ssl.wrap_socket(sock)
        self.sock.connect(server)

    def request(self, req, buf=''):
        self.sock.sendall(''.join([
            struct.pack('!Q', len(req) + len(buf)),
            struct.pack('!H', len(req)),
            req,
            buf]))

        def recvall(length):
            pkt = list()
            while length > 0:
                pkt.append(self.sock.recv(length))
                if 0 == len(pkt[-1]):
                    raise Exception('disconnected')
                length -= len(pkt[-1])
            return ''.join(pkt)

        return recvall(struct.unpack('!Q', recvall(8))[0]+2)[2:]
