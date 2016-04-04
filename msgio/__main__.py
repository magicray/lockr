import sys
import epoll

epoll.loop(__import__(sys.argv[1] + '.msgio', fromlist=sys.argv[1]))
