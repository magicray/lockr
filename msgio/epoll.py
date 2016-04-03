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


def loop(module, node, port, peers, certfile):
    tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tmp_sock.setblocking(0)
    try:
        tmp_sock.connect(('0.0.0.0', port))
    except:
        local_ip = tmp_sock.getsockname()[0]

    if node is None:
        node = (local_ip, port)

    clients = set()
    for p in peers:
        if type(p) is not tuple:
            x = p.split(':')[0], p.split(':')[1]
        addr = (socket.gethostbyname(x[0]), int(x[1]))

        if addr > (local_ip, port):
            clients.add((socket.gethostbyname(x[0]), int(x[1])))

    listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener_sock.setblocking(0)
    listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener_sock.bind((local_ip, port))
    listener_sock.listen(5)
    logging.critical('listening on {0}'.format(port))

    epoll = select.epoll()
    epoll.register(listener_sock.fileno(), select.EPOLLIN)

    stats = dict(in_pkt=0, in_bytes=0, out_pkt=0, out_bytes=0,
                 srv_accept=0, srv_established=0, srv_disconnect=0,
                 cli_connect=0, cli_established=0, cli_disconnect=0)

    connections = dict()
    addr2fd = dict()
    key = os.getenv('MSGIO_KEY', '')

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

                        if '' == msg:
                            cred = marshal.loads(buf)
                            assert(key == cred['key'])
                            conn['src'] = cred['node']
                            addr2fd[conn['src']] = fileno

                            if conn['is_server'] is True:
                                out_msg_list.append(dict(msg='',
                                    buf=marshal.dumps(dict(
                                        node=node, key=key))))
                        try:
                            if '' == msg:
                                out_msg_list.append(module.on_connect(
                                    conn['src']))
                            else:
                                src = conn.get('src', conn['peer'])
                                out_msg_list.append(module.on_message(
                                    src, msg, buf))
                        except Exception as e:
                            conn['close'] = (e, traceback.format_exc())
                            raise

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
                    addr2fd[conn['peer']] = fileno

                    if conn['is_server'] is False:
                        out_msg_list.append(dict(msg='', buf=marshal.dumps(
                            dict(node=node, key=key))))

                if event & ~(select.EPOLLIN | select.EPOLLOUT):
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
                        out_msg_list.append(module.on_disconnect(
                            conn['src'], exc, tb))

                    if conn['is_server']:
                        stats['srv_disconnect'] += 1
                    else:
                        stats['cli_disconnect'] += 1
                        clients.add(conn['ip_port'])

                if out_msg_list:
                    for l in filter(lambda x: x is not None, out_msg_list):
                        for d in l if type(l) is list else [l]:
                            dst = d.get('dst', conn['peer'])
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

        getattr(module, 'on_stats', lambda x: None)(copy.deepcopy(stats))
