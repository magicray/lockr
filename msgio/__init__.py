import ssl
import socket
import struct


servers = dict()


def request(srv, req, buf=''):
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

        return recvall(struct.unpack('!Q', recvall(8))[0]+2)[2:]
    except:
        if srv in servers:
            servers.pop(srv).close()
        raise
