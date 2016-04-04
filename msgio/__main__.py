import os
import sys
import epoll

module = __import__(sys.argv[1] + '.msgio', fromlist=sys.argv[1])

conf = module.on_init()

certfile = conf.get('cert', 'cert.pem')
if not os.path.isfile(certfile):
    os.mknod(certfile)
    os.system('openssl req -new -x509 -days 365 -nodes -newkey rsa:2048 -subj '
              '"/" -out {0} -keyout {0} 2> /dev/null'.format(certfile))

epoll.loop(
    module,
    conf.get('node'),
    conf.get('port', 1234),
    conf.get('peers', list()),
    certfile)
