# from https://github.com/jstasiak/python-zeroconf/blob/master/examples/registration.py
""" Example of announcing a service (in this case, a fake HTTP server) """

import argparse
import logging
import socket
import sys
from time import sleep

from zeroconf import IPVersion, ServiceInfo, Zeroconf

desc = {}
serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


serversocket.setblocking(True)
hostname = socket.gethostname()

# This accepts a tuple...
port = 0  # next available
serversocket.bind((socket.gethostname(), port))
IPAddr = serversocket.getsockname()[0]
externalPort = serversocket.getsockname()[1]
print("listening on interface " + hostname + " port " + str(externalPort) + " aka " + IPAddr)


info = ServiceInfo(
    "_magicdice._udp.local.",
    "Rob's totally fake server._magicdice._udp.local.",
    # robin.cs.umanitoba.ca
    addresses=[socket.inet_aton("130.179.28.129")],
    # addresses=[socket.inet_aton("127.0.0.1")],
    port=externalPort,
    properties=desc,
    server="something_unique.local.",
)

zeroconf = Zeroconf()
print("Registration of a service, press Ctrl-C to exit...")
zeroconf.register_service(info)
try:
    while True:
        data, addr = serversocket.recvfrom(1024)
        print("got >{}< from {}".format(data, addr))
        serversocket.sendto(b'The word of the day is >babar<', addr)
except KeyboardInterrupt:
    pass
finally:
    print("Unregistering...")
    zeroconf.unregister_service(info)
    zeroconf.close()

