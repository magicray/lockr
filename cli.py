import cmd
import json
import shlex
import client
import pprint
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
        print(pprint.pformat(self.cli.state()).replace("u'", " '"))

    def do_get(self, line):
        key_ver = dict([(k, 0) for k in shlex.split(line)])
        code, offset, result = self.cli.get(key_ver)
        assert(0 == code)
        for k in sorted(result.keys()):
            print('{0} <{1}> {2}'.format(k, result[k][0], result[k][1]))

    def do_put(self, line):
        cmd = shlex.split(line)
        tup = zip(cmd[0::3], cmd[1::3], cmd[2::3])
        docs = dict([(t[0], (int(t[1]), t[2])) for t in tup])
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
        while True:
            for i in range(100000):
                while True:
                    key = '%05d' % (i)
                    code, offset, result = self.cli.get({key: 0})
                    ver, res = result.get(key, (0, '{}'))

                    doc = json.loads(res)
                    doc[test_id] = doc.get(test_id, 0) + 1
                    doc = json.dumps(doc, indent=4, sort_keys=True)

                    res = self.cli.put({key: (ver, doc)})
                    logger.critical((key, ver, res))
                    if 0 == res[0]:
                        break
