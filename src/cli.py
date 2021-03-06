#!/usr/bin/python3

import sys
import socket


if len(sys.argv) < 3:
    print("to run the CLI: python3 cli.py <host> <port>")

HOSTNAME = sys.argv[1]
PORT = int(sys.argv[2])

tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# tcp_socket.setblocking(False)
tcp_socket.connect((HOSTNAME, PORT))

try:
    while True:

        msg = sys.stdin.readline()
        msg = msg.strip()  # remove \r\n from stdin

        if msg:
            tcp_socket.send(msg.encode('utf-8'))
            data = tcp_socket.recv(4096).decode('utf-8')
            print(data)

            if msg.lower() == 'exit':
                break


except KeyboardInterrupt:
    print('[CLI] Keyboard Interrupt')
    pass
except Exception as e:
    print("[CLI] Program Exception : {}".format(e))
finally:
    tcp_socket.close()
    print('[CLI] Closing all connections...')
