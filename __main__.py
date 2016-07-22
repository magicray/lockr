import os
import sys
import yaml
import time
import fcntl
import signal
import random
import logging
import traceback


def get_conf():
    conf = dict(
        key='key',
        cert='ssl.cert',
        data='data',
        index='index.db',
        max_size=2**14,
        repl_size=2**20,
        timeout=600)

    with open('conf.yaml') as fd:
        conf.update(yaml.load(fd.read()))

    conf['nodes'] = set(map(lambda x: (x.split(':')[0], int(x.split(':')[1])),
                            conf['nodes']))

    if 'port' in conf:
        ip, port = conf['port'].split(':')
        conf['port'] = (ip, int(port))

    return conf

if __name__ == '__main__':
    logging.basicConfig(level=0, format='%(asctime)s: %(message)s')

    if 'port' in get_conf():
        fcntl.flock(os.open('.', os.O_RDONLY), fcntl.LOCK_EX | fcntl.LOCK_NB)

        os.close(0)
        os.close(1)

        if len(sys.argv) > 1:
            if os.fork():
                os._exit(0)

            os.setsid()

        while True:
            if 0 == os.fork():
                import msgio
                import server

                if len(sys.argv) > 1:
                    os.dup2(os.open('{0}.{1}'.format(
                        sys.argv[1],
                        time.strftime('%y%m%d', time.gmtime())),
                        os.O_CREAT | os.O_WRONLY | os.O_APPEND), 2)

                logging.critical('')
                try:
                    conf = get_conf()
                    server.init(conf)

                    signal.alarm(random.randint(
                        conf['timeout'], 2*conf['timeout']))

                    msgio.loop(server, conf['port'], conf['nodes'],
                               conf['key'], conf['cert'])
                except:
                    signal.alarm(0)
                    logging.critical(traceback.format_exc())
                    time.sleep(10**8)

                os._exit(0)

            os.wait()
    else:
        import cli
        cli.Client(get_conf()['nodes']).cmdloop()
