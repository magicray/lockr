import os
import sys
import ssl
import time
import copy
import socket
import select
import struct
import logging
import traceback


def request(srv, req, buf=''):
    servers = getattr(request, 'servers')

    try:
        if not req:
            raise Exception('closed')

        if srv not in servers:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock = ssl.wrap_socket(sock)
            sock.connect(srv)
            servers[srv] = sock

        servers[srv].sendall(''.join([
            struct.pack('!Q', len(req) + len(buf)),
            struct.pack('!H', len(req)),
            req,
            buf]))

        def recvall(length):
            pkt = list()
            while length > 0:
                pkt.append(servers[srv].recv(length))
                if 0 == len(pkt[-1]):
                    raise Exception('disconnected')
                length -= len(pkt[-1])
            return ''.join(pkt)

        return recvall(struct.unpack('!Q', recvall(8))[0] + 2)[2:]
    except:
        if srv in servers:
            servers.pop(srv).close()
        raise

request.servers = dict()


class msgio_exception(Exception):
    def __init__(self, exc, tb):
        self.exc = exc
        self.tb = tb


def loop(module, port, msgs, certfile):
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
    clients = set(filter(lambda x: x > port, msgs))

    old_stats = (time.time(), copy.deepcopy(stats))

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
                                    certfile=certfile,
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
                out_msg_list = list()

                if conn['handshake_done'] and event & select.EPOLLIN:
                    conn['in_pkts'].append(conn['sock'].recv(32*1024))
                    stats['in_bytes'] += len(conn['in_pkts'][-1])
                    assert(len(conn['in_pkts'][-1]) > 0)

                    size = struct.unpack('!Q', conn['in_pkts'][0][0:8])[0] + 10

                    total_size = sum(map(len, conn['in_pkts']))
                    assert(total_size <= size)

                    if size == total_size:
                        msg_len = struct.unpack(
                            '!H', conn['in_pkts'][0][8:10])[0]
                        msg = conn['in_pkts'][0][10:10+msg_len]

                        conn['in_pkts'][0] = conn['in_pkts'][0][10+msg_len:]
                        buf = ''.join(conn['in_pkts'])
                        try:
                            if '' == msg:
                                conn['src'] = (conn['peer'][0], int(buf))
                                addr2fd[conn['src']] = fileno
                                assert(conn['src'] in msgs)
                                out_msg_list.append(msgs[conn['src']])
                            else:
                                out_msg_list.append(module.on_message(
                                    conn['src'], msg, buf))
                        except Exception as e:
                            raise msgio_exception(e, traceback.format_exc())

                        stats['in_pkt'] += 1
                        conn['in_pkts'] = list()

                if conn['handshake_done'] and event & select.EPOLLOUT:
                    if 'pkt' not in conn:
                        if conn['msgs']:
                            conn['pkt'] = conn['msgs'].pop(0)
                            conn['sent'] = 0

                    if 'pkt' in conn:
                        if len(conn['pkt']) > conn['sent']:
                            n = conn['sock'].send(conn['pkt'][
                                conn['sent']:conn['sent']+32*1024])

                            conn['sent'] += n
                            stats['out_bytes'] += n

                        if len(conn['pkt']) == conn['sent']:
                            del(conn['pkt'])
                            del(conn['sent'])
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
                    conn['in_pkts'] = list()
                    conn['msgs'] = list()
                    conn['src'] = conn['peer']
                    addr2fd[conn['src']] = fileno

                    try:
                        if not conn['is_server']:
                            out_msg_list.extend([
                                dict(buf=str(port[1])),
                                msgs[conn['src']]])
                    except Exception as e:
                        raise msgio_exception(e, traceback.format_exc())

                if event & ~(select.EPOLLIN | select.EPOLLOUT):
                    raise Exception('unhandled event({0})'.format(event))

            except ssl.SSLError as e:
                if ssl.SSL_ERROR_WANT_READ == e.errno:
                    epoll.modify(fileno, select.EPOLLIN)
                elif ssl.SSL_ERROR_WANT_WRITE == e.errno:
                    epoll.modify(fileno, select.EPOLLOUT)
                else:
                    conn['close'] = (None, traceback.format_exc())
            except msgio_exception as e:
                conn['close'] = (e.exc, e.tb)
            except Exception as e:
                conn['close'] = (None, traceback.format_exc())
            finally:
                if 'close' in conn:
                    exc, tb = conn['close']

                    conn = connections.pop(fileno)

                    conn['sock'].close()
                    epoll.unregister(fileno)

                    addr2fd.pop(conn.get('src'), None)

                    if conn['handshake_done']:
                        out_msg_list.append(module.on_error(
                            conn['src'], exc, tb))

                    if conn['is_server']:
                        stats['srv_disconnect'] += 1
                    else:
                        stats['cli_disconnect'] += 1
                        clients.add((conn['ip_port'][0], conn['ip_port'][1]))

                if out_msg_list:
                    for l in filter(lambda x: x is not None, out_msg_list):
                        for d in l if type(l) is list else [l]:
                            dst = d.get('dst', conn['src'])
                            msg = d.get('msg', '')
                            buf = d.get('buf', '')

                            if dst not in addr2fd:
                                logging.critical('{0} not found'.format(dst))
                                continue

                            f = addr2fd[dst]
                            connections[f]['msgs'].append(''.join([
                                struct.pack('!Q', len(msg) + len(buf)),
                                struct.pack('!H', len(msg)),
                                msg,
                                buf]))
                            epoll.modify(f, select.EPOLLIN | select.EPOLLOUT)

                if conn['handshake_done'] is True and 'close' not in conn:
                    if conn['msgs'] or ('pkt' in conn):
                        epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT)
                    else:
                        epoll.modify(fileno, select.EPOLLIN)

        if time.time() > old_stats[0] + 60:
            divisor = (time.time() - old_stats[0])*2**20
            logging.info('in-mbps(%0.3f) out-mbps(%0.3f)' % (
                8*(stats['in_bytes'] - old_stats[1]['in_bytes'])/divisor,
                8*(stats['out_bytes'] - old_stats[1]['out_bytes'])/divisor))
            old_stats = (time.time(), copy.deepcopy(stats))

        getattr(module, 'on_stats')(copy.deepcopy(stats))


if '__main__' == __name__:
    module = __import__(
        sys.argv[1],
        fromlist='.'.join(sys.argv[1].split('.')[:-1]))

    conf = module.on_init()

    certfile = conf.get('cert', 'cert.pem')
    if not os.path.isfile(certfile):
        os.mknod(certfile)
        os.system(
            'openssl req -new -x509 -days 365 -nodes -newkey rsa:2048 '
            ' -subj "/" -out {0} -keyout {0} 2> /dev/null'.format(certfile))

    loop(module,
         conf.get('port', ('0.0.0.0', 1234)),
         conf.get('msgs', list()),
         certfile)
