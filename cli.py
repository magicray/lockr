import cmd
import json
import shlex
import client
import logging
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


class Client(cmd.Cmd):
    prompt = '>'

    def __init__(self, servers):
        cmd.Cmd.__init__(self)
        self.cli = client.Lockr(servers)

    def do_EOF(self, line):
        self.do_quit(line)

    def do_quit(self, line):
        exit(0)

    def do_state(self, line):
        state = self.cli.state()

        for k in filter(lambda k: type(state[k]) is dict, state):
            if k == 'vclock':
                continue

            if state[k]['vclock'] == state['vclock']:
                state[k].pop('vclock')

            if state[k]['state'] == 'following-{0}'.format(state['node']):
                state[k].pop('state')

        state.pop('state')

        print(json.dumps(state, indent=4, sort_keys=True))

    def do_get(self, line):
        key_ver = dict([(k, 0) for k in shlex.split(line)])
        code, offset, result = self.cli.get(key_ver)
        assert(0 == code)
        for k in sorted(result.keys()):
            print('{0} <{1}> {2}'.format(k, result[k][0], result[k][1]))

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::4], cmd[1::4], cmd[2::4], cmd[3::4])
        docs = dict([(t[0], (int(t[1]), int(t[2]), t[3])) for t in tup])
        code, value = self.cli.put(docs)
        if 0 == code:
            print('committed : {0}'.format(value))
        elif 1 == code:
            print('version mismatch for the following keys:-')
            for k, v in value.iteritems():
                if v:
                    print('{0} <{1}>'.format(k, v))
        else:
            print(value)

    def do_watch(self, line):
        while True:
            try:
                for result in self.cli.watch(shlex.split(line)):
                    for k in sorted(result['added'].keys()):
                        print('ADD {0} - {1}'.format(k, result['added'][k]))
                    for k in sorted(result['updated'].keys()):
                        print('MOD {0} - {1}'.format(k, result['updated'][k]))
                    for k in sorted(result['deleted']):
                        print('DEL {0}'.format(k))
            except:
                traceback.print_exc()

    def do_test(self, line):
        test_id = '%06d' % (int(line))
        for i in range(100000):
            while True:
                key = '%05d' % (i)
                code, offset, result = self.cli.get({key: 0})
                ver, res = result.get(key, (0, '{}'))

                doc = json.loads(res)
                doc[test_id] = doc.get(test_id, 0) + 1
                doc = json.dumps(doc, indent=4, sort_keys=True)

                res = self.cli.put({key: (ver, 0, doc)})
                logger.critical((key, ver, res))
                if 0 == res[0]:
                    break
