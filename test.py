import sys
import select
import json
import socket
import time
import random
import uuid
import re
from zeroconf import IPVersion, ServiceInfo, Zeroconf, ServiceBrowser

peer1 = {
    'host': '1.1.1.1',
    'port': 8000,
    'name': "peer 1"
}

peer2 = {
    'host': '1.1.1.2',
    'port': 8000,
    'name': "peer 2"
}

peer2_test = {
    'host': '1.1.1.2',
    'port': 8000,
    'name': "test"
}

peers = [peer1, peer2]

exst_peer = [peer for peer in peers if '1.1.1.2' ==
             peer['host'] and 8000 == peer['port']]
print(exst_peer)
exst_peer = exst_peer.pop()
exst_peer['name'] = "yada yada yada"
print(exst_peer)

# print(peer1)
# print(json.dumps(peers).encode('utf-8'))

# print(time.time())

# How to get time difference
# last_seen = 1627755420.5782158
# time_now = time.time()
# diff = time_now - last_seen
# print("Last seen: ", time.ctime(last_seen))
# print("Now      : ", time.ctime())

# print(diff)
# print(time.ctime(diff))

# curr_time = time.localtime()
# print(curr_time)
# print(time.localtime(time.time()))
