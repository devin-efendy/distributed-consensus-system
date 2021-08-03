import sys
import socket
import select


if len(sys.argv) < 3:
    print("to run the CLI: python3 cli.py <host> <port>")

HOSTNAME = sys.argv[1]
PORT = int(sys.argv[2])

try:
    while True:
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # tcp_socket.setblocking(False)
        tcp_socket.connect((HOSTNAME, PORT))

        msg = sys.stdin.readline()
        msg = msg.strip()  # remove \r\n from stdin

        tcp_socket.send(msg.encode('utf-8'))
        data = tcp_socket.recv(4096).decode('utf-8')
        print(data)

        tcp_socket.close()

        if msg.lower() == 'exit':
            print('break....')
            break

except KeyboardInterrupt:
    print('[CLI] Keyboard Interrupt')
    pass
except Exception as e:
    print("[CLI] Program Exception : {}".format(e))
finally:
    print('[CLI] Closing all connections...')
