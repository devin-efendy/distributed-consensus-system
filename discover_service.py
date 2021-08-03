# from https://github.com/jstasiak/python-zeroconf/blob/master/examples/browser.py

from zeroconf import ServiceBrowser, Zeroconf
import socket

class MyListener:

    def remove_service(self, zeroconf, type, name):
        print("Service %s removed" % (name,))

    def add_service(self, zeroconf, type, name):
        try:
            info = zeroconf.get_service_info(type, name)
            print("Service %s added, service info: %s" % (name, info))
            ip = socket.inet_ntoa(info.addresses[0])
            print(ip)
            serversocket.sendto(b'dingo', (ip, info.port))
        except Exception as e:
            print(e)

    def update_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        print("Service %s updated, service info: %s" % (name, info))


# ready the socket!
serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serversocket.setblocking(True)
hostname = socket.gethostname()
# This accepts a tuple...
port = 0  # next available
serversocket.bind((socket.gethostname(), port))


zeroconf = Zeroconf()
listener = MyListener()
# http://wiki.ros.org/zeroconf/Tutorials/Understanding%20Zeroconf%20Service%20Types
# http://www.dns-sd.org/ServiceTypes.html
#browser = ServiceBrowser(zeroconf, "_http._tcp.local.", listener)
browser = ServiceBrowser(zeroconf, "_magicdice._udp.local.", listener)
try:
    while True:
        data, addr = serversocket.recvfrom(1024)
        print("got >{}< from {}".format(data, addr))
    input("Press enter to exit...\n\n")
finally:
    zeroconf.close()

