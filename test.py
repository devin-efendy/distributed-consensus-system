#!/usr/bin/python3

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


def test():
    global peers
    exst_peer = [peer for peer in peers if '1.1.1.2' ==
                 peer['host'] and 8000 == peer['port']]
    exst_peer = exst_peer[0]
    exst_peer['name'] = "yada yada yada"


def parse_peers_addr(list):
    res = []
    for p in list:
        addr = p.split(':')
        res.append((addr[0], addr[1]))

    return res


def most_frequent(d):
    result = ''
    max_count = 0

    for word in d:
        count = d.get(word)
        if count >= max_count:
            result = word
            max_count = count

    return result


s = "123:456"

print(s.split(":"))
print(s)
