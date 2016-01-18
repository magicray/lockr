import os
import ssl
import socket
import select
import struct
import hashlib
import inspect
import optparse
import traceback


def epoll_loop(module, port, clients):
    callbacks = dict()
    for m in inspect.getmembers(module, inspect.isfunction):
        if m[0].startswith('callback_'):
            callbacks[hashlib.sha1(m[0][9:]).digest()] = getattr(module, m[0])

    listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener_sock.bind(port)
    listener_sock.listen(5)
    listener_sock.setblocking(0)

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
                s.connect(ip_port)
            except Exception as e:
                if 115 != e.errno:
                    raise

        def handle_out_messages(msg_list, peer):
            if not msg_list:
                msg_list = []
            elif type(msg_list) is dict:
                msg_list = [msg_list]

            for d in msg_list:
                msg_to = d.get('peer', peer)
                msg_type = d.get('type', 'default')
                msg = d.get('msg', '')

                if msg_to in addr2fd:
                    f = addr2fd[msg_to]
                    connections[f]['msgs'].append(''.join([
                        hashlib.sha1(msg_type).digest(),
                        struct.pack('!I', len(msg))]))
                    connections[f]['msgs'].append(msg)
                    epoll.modify(f, select.EPOLLIN | select.EPOLLOUT)

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

                if False == conn['handshake_done']:
                    if getattr(conn['sock'], 'do_handshake', None) is None:
                        conn['sock'] = ssl.wrap_socket(
                            conn['sock'],
                            do_handshake_on_connect=False,
                            server_side=False)
                    try:
                        conn['sock'].do_handshake()
                        conn['handshake_done'] = True
                        conn['peer'] = conn['sock'].getpeername()
                        conn['in_hdr_size'] = 24
                        conn['in_hdr_pkts'] = list()
                        conn['msgs'] = list()
                        addr2fd[conn['peer']] = fileno
                        if conn['is_server']:
                            method = getattr(module, 'on_accept')
                        else:
                            method = getattr(module, 'on_connect')

                        epoll.modify(fileno, select.EPOLLIN)

                        handle_out_messages(
                            method(conn['peer']),
                            conn['peer'])

                    except ssl.SSLError as e:
                        if ssl.SSL_ERROR_WANT_READ == e.errno:
                            epoll.modify(fileno, select.EPOLLIN)
                        elif ssl.SSL_ERROR_WANT_WRITE == e.errno:
                            epoll.modify(fileno, select.EPOLLOUT)
                        else:
                            raise
                    continue

                epoll.modify(fileno, select.EPOLLIN)
                if event & select.EPOLLIN:
                    if 'in_size' not in conn:
                        read_bytes = conn['in_hdr_size']
                    else:
                        read_bytes = conn['in_size']

                    try:
                        buf = conn['sock'].recv(read_bytes)
                        stats['in_bytes'] += len(buf)
                        assert(len(buf) > 0)
                    except:
                        raise Exception('closed by peer')

                    if 'in_size' not in conn:
                        conn['in_hdr_pkts'].append(buf)
                        conn['in_hdr_size'] -= len(buf)
                        if 0 == conn['in_hdr_size']:
                            b = ''.join(conn['in_hdr_pkts'])
                            name = b[0:20]
                            size = struct.unpack('!I', b[20:])[0]
                            if 0 == size:
                                stats['in_pkt'] += 1
                                handle_out_messages(
                                    callbacks[name](conn['peer'], ''),
                                    conn['peer'])
                            else:
                                conn['in_size'] = size
                                conn['in_pkts'] = list()
                                conn['name'] = name
                            conn['in_hdr_size'] = 24
                            conn['in_hdr_pkts'] = list()
                    else:
                        conn['in_pkts'].append(buf)
                        conn['in_size'] -= len(buf)
                        if 0 == conn['in_size']:
                            pkt = ''.join(conn['in_pkts'])
                            del(conn['in_size'])
                            del(conn['in_pkts'])
                            stats['in_pkt'] += 1
                            handle_out_messages(
                                callbacks[conn['name']](conn['peer'], pkt),
                                conn['peer'])

                if event & select.EPOLLOUT:
                    if 'pkt' not in conn:
                        if conn['msgs']:
                            conn['pkt'] = conn['msgs'].pop(0)
                            conn['sent'] = 0

                    if 'pkt' in conn:
                        if len(conn['pkt']) > conn['sent']:
                            try:
                                pkt = conn['pkt']
                                n = conn['sock'].send(pkt[conn['sent']:])
                            except:
                                raise Exception('closed by peer')

                            conn['sent'] += n
                            stats['out_bytes'] += n

                        if len(conn['pkt']) == conn['sent']:
                            del(conn['pkt'])
                            del(conn['sent'])
                            stats['out_pkt'] += 1

                        if conn['msgs'] or ('pkt' in conn):
                            epoll.modify(fileno,
                                         select.EPOLLIN | select.EPOLLOUT)

                if event & ~(select.EPOLLIN | select.EPOLLOUT):
                    raise Exception('unhandled event({0})'.format(event))

            except Exception as e:
                conn = connections.pop(fileno)
                if conn['handshake_done'] and (str(e) != 'closed by peer'):
                    traceback.print_exc()
                    exit(1)
                conn['sock'].close()
                epoll.unregister(fileno)

                addr2fd.pop(conn.get('peer'), None)

                if conn['is_server']:
                    getattr(module, 'on_reject')(conn['peer'])
                    stats['srv_disconnect'] += 1
                else:
                    getattr(module, 'on_disconnect')(conn['ip_port'])
                    stats['cli_disconnect'] += 1
                    clients.add((conn['ip_port'][0], conn['ip_port'][1]))

        getattr(module, 'on_stats')(stats)


if '__main__' == __name__:
    parser = optparse.OptionParser()
    parser.add_option('-b', '--bind', dest='port', type='string',
                      help='server:port tuple. skip to start the client')
    parser.add_option('-c', '--cert', dest='cert', type='string',
                      help='certificate file path', default='cert.pem')
    parser.add_option('-s', '--servers', dest='servers', type='string',
                      help='comma separated list of ip:port')
    parser.add_option('-m', '--module_name', dest='module', type='string',
                      help='module name')
    parser.add_option('--conf', dest='conf', type='string',
                      help='configuration file')
    opt, args = parser.parse_args()

    os.system('openssl req -new -x509 -days 365 -nodes -newkey rsa:2048 '
              ' -subj "/" -out cert.pem -keyout cert.pem 2> /dev/null')

    servers = list()
    if opt.servers:
        servers = set(map(lambda x: (socket.gethostbyname(x[0]), int(x[1])),
                          map(lambda x: x.split(':'),
                              opt.servers.split(','))))
    port = (socket.gethostbyname(opt.port.split(':')[0]),
            int(opt.port.split(':')[1]))

    module = __import__(opt.module)
    module.on_init(port, servers, opt.conf)
    epoll_loop(module, port, servers)
