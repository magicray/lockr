import json
import random
import logging


def callback_random_packet(src, buf):
    logging.critical('received len({0})'.format(len(buf)))
    return dict(type='random_packet', buf=' '*(int(random.random()*10*2**20)))


def on_connect(src):
    logging.critical('connected to {0}'.format(src))
    return dict(type='random_packet')


def on_disconnect(src):
    logging.critical('disconnected from {0}'.format(src))


def on_accept(src):
    logging.critical('accepted connection from {0}'.format(src))


def on_reject(src):
    logging.critical('rejected connection from {0}'.format(src))


def on_stats(stats):
    logging.info(json.dumps(stats, indent=4, sort_keys=True))


def on_init(port, servers, conf):
    logging.critical('on_init')
