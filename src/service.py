"""

TODO:

1. Handle CLI requests from cli.py (50% complete, implement UI + multi-client)

(This part ordered from easy to the most difficult)
4. Implement SET
5. Implement FLOOD

"""

from os import write
import sys
import select
import json
import socket
import time
import random
import uuid
import re
from zeroconf import IPVersion, ServiceInfo, Zeroconf, ServiceBrowser

A3_NETWORK_HOST = 'silicon.cs.umanitoba.ca'
A3_NETWORK_PORT = 16000
A3_ADDR = (A3_NETWORK_HOST, A3_NETWORK_PORT)

CMD_FLOOD_ = 'FLOOD'
CMD_FLOOD_REPLY_ = 'FLOOD-REPLY'
CMD_CONSENSUS_ = 'CONSENSUS'
CMD_CONSENSUS_REPLY_ = 'CONSENSUS-REPLY'
CMD_QUERY_ = 'QUERY'
CMD_QUERY_REPLY_ = 'QUERY-REPLY'
CMD_SET_ = 'SET'

CLI_PEERS_ = 'peers'
CLI_CURRENT_ = 'current'
CLI_CONSENSUS_ = 'consensus'
CLI_LIE_ = 'lie'
CLI_TRUTH_ = 'truth'
CLI_SET_ = 'set'
CLI_EXIT_ = 'exit'

# ================== UDP Socket for A3 peer-to-peer network ==================
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_socket.setblocking(False)

udp_hostname = socket.gethostname()

# This accepts a tuple...
udp_port = 0  # next available
udp_socket.bind((udp_hostname, udp_port))

ip_addr = udp_socket.getsockname()[0]
external_port = udp_socket.getsockname()[1]

print("[UDP] Listening on interface " + udp_hostname +
      " port " + str(external_port) + " aka " + ip_addr)

NODE_HOST = socket.gethostbyname(socket.gethostname())
NODE_PORT = external_port
NODE_ADDR = (NODE_HOST, NODE_PORT)

# ================== TCP Socket for CLI ==================

tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# tcp_socket.setblocking(False)

tcp_hostname = socket.gethostname()
tcp_port = 8023

tcp_socket.bind((tcp_hostname, tcp_port))
tcp_socket.listen(5)

print("[TCP] Listening on interface " + tcp_hostname +
      " port " + str(tcp_port))

# list of KNOWN peers
peers = []
message_queue = []
words_db = []


def drop_inactive_nodes():
    # for peer in peers:
    #     last_active = peer['last_active']
    #     time_diff = time.localtime(time.time() - last_active)

    #     if time_diff.tm_min >= 2:
    #         peers.remove(peer)
    pass


def is_node_inactive(node):
    return time.time() - node['last_active'] >= 120


def join_network(udp_socket):
    """
    FLOOD message template
    use udp_socket.sendto(message, address) to send the first FLOOD message
    Then, join the networks
    """

    flood_message = {
        "command": CMD_FLOOD_,
        "host": NODE_HOST,
        "port": NODE_PORT,
        "name": "Test's peer on {}".format(socket.gethostname()),
        "messageID": str(uuid.uuid4())
    }

    print(
        "======================== [DEBUG] - FLOODMESSAGE ========================")
    print(flood_message)

    flood_message = json.dumps(flood_message).encode('utf-8')
    udp_socket.sendto(flood_message, A3_ADDR)
    pass


def cmd_flood(udp_socket, message, addr):
    print(message)

    msg_name = message['name']
    msg_host = message['host']
    msg_port = message['port']

    # Bad message
    if not (msg_host and msg_port and msg_name):
        return None

    # Some Nodes might send back our own FLOOD message
    if NODE_ADDR == (msg_host, msg_port):
        return None

    # New node in the network, add it into our peer list
    if not any(msg_host == peer['host'] and msg_port == peer['port'] for peer in peers):
        new_peer = {
            'name': msg_name,
            'host': msg_host,
            'port': msg_port,
            'database': [],
            'last_active': time.time()
        }

        peers.append(new_peer)

        reply_message = {
            "command": CMD_FLOOD_REPLY_,
            "host": NODE_HOST,
            "port": NODE_PORT,
            "name": "Test's peer on {}".format(socket.gethostname())
        }
        reply_message = json.dumps(reply_message).encode('utf-8')
        udp_socket.sendto(reply_message, (msg_host, msg_port))
        print("[DEBUG] - FLOOD Peer never been seen")
        print(NODE_ADDR, (msg_host, msg_port))
    else:
        # update the last_active time for the existing node
        existing_peer = [peer for peer in peers if msg_host ==
                         peer['host'] and msg_port == peer['port']]

        existing_peer = existing_peer.pop()
        existing_peer['last_active'] = time.time()

    # for peer in peers:
    #     peer_host = peer['host']
    #     peer_port = peer['port']

    #     if ((msg_host, msg_port) != (peer_host, peer_port) and
    #             NODE_ADDR != (msg_host, msg_port)):
    #         flood_msg = json.dumps(message).encode('utf-8')
    #         udp_socket.sendto(flood_msg, (peer_host, peer_port))

    pass


def cmd_flood_reply(udp_socket, message, addr):
    print(message)

    # The purpose for FLOOD-REPLY is to add all the peers in the network
    host_ = message['host']
    port_ = message['port']
    name_ = message['name']

    if host_ and port_:
        peer = {
            'name': name_,
            'host': host_,
            'port': port_,
            'database': [],
            'last_active': time.time()
        }

        # Check if the address is not yourself (malicious messages from other peers, yes I have trust issues)
        # Check if you REALLY never see this peer at all
        if (NODE_ADDR != (host_, port_) and
                not any((host_, port_) == (
                    peer['host'], peer['port']) for peer in peers)
            ):
            peers.append(peer)
            print(peers)
    pass


def cmd_consensus(udp_socket, message, addr):
    # print("===== CONSENSUS =====")
    # print(message)
    pass


def cmd_consensus_reply(udp_socket, message, addr):
    # print("===== CONSENSUS-REPLY =====")
    # print(message)
    pass


def cmd_query(udp_socket, message, addr):
    query_message = {
        "command": CMD_QUERY_
    }
    query_message = json.dumps(query_message).encode('utf-8')
    udp_socket.sendto(query_message, A3_ADDR)
    pass


def cmd_query_reply(udp_socket, message, addr):

    result = res_msg['database']

    if result:
        words_db = result
        # print(words_db)
        print("===== QUERY-REPLY =====")
        print(words_db)
    pass


def cmd_set(udp_socket, message, addr):
    # print("===== SET =====")
    # print(message)
    pass

# CLI functions


def cli_peers(message):
    print(message)
    response = ''

    for peer in peers:
        peer_txt = '{} | {}:{} | {} | {}'.format(
            peer['name'],
            peer['host'],
            peer['port'],
            peer['database'],
            time.ctime(peer['last_active'])
        )

        response += "{}\r\n".format(peer_txt)

    return response


def cli_current(message):
    print(message)

    return "[NODE_REPLY] current"


def cli_consensus(message):
    print(message)

    return "[NODE_REPLY] consensus"


def cli_lie(message):
    print(message)

    return "[NODE_REPLY] lie"


def cli_truth(message):
    print(message)

    return "[NODE_REPLY] truth"


def cli_set(message):
    print(message)

    return "[NODE_REPLY] set"


def cli_exit(client_conneciton):
    client_conneciton.close()
    print("[NODE] Closing CLI connection...")
    return "Server has closed the connection..."


handle_commands = dict()
handle_commands[CMD_FLOOD_] = cmd_flood
handle_commands[CMD_FLOOD_REPLY_] = cmd_flood_reply
handle_commands[CMD_CONSENSUS_] = cmd_consensus
handle_commands[CMD_CONSENSUS_REPLY_] = cmd_consensus_reply
handle_commands[CMD_QUERY_] = cmd_query
handle_commands[CMD_QUERY_REPLY_] = cmd_query_reply
handle_commands[CMD_SET_] = cmd_set

handle_cli_commands = dict()
handle_cli_commands[CLI_PEERS_] = cli_peers
handle_cli_commands[CLI_CURRENT_] = cli_current
handle_cli_commands[CLI_CONSENSUS_] = cli_current
handle_cli_commands[CLI_LIE_] = cli_lie
handle_cli_commands[CLI_TRUTH_] = cli_truth
handle_cli_commands[CLI_SET_] = cli_set
handle_cli_commands[CLI_EXIT_] = cli_exit


# ================== Register Service ==================
desc = {}

info = ServiceInfo(
    '_magicdice._udp.local.',
    "Test's peer._magicdice._udp.local.",
    addresses=[socket.inet_aton(socket.gethostbyname(socket.gethostname()))],
    port=external_port,
    properties={},
    server="something_unique.local.",
)

zeroconf = Zeroconf()
zeroconf.register_service(info)


inputs = [udp_socket, tcp_socket]
outputs = []  # None

join_network(udp_socket)
cmd_query(udp_socket, '', ())

last_flood_msg = time.time()

try:
    while True:
        # Need to calculate the timeout in someway...
        # This requirement is pretty BS...
        timeout_sec = 60 - (time.time() - last_flood_msg)

        readable, writable, exceptional = select.select(
            inputs, outputs, inputs, timeout_sec)

        #
        # if timeout_sec <= 0 then it's time to join the network again
        if(time.time() - last_flood_msg > 60):
            join_network(udp_socket)
            last_flood_msg = time.time()

        # Read what we can, from where we can
        for source in readable:
            if source is udp_socket:
                data, addr = source.recvfrom(1024)
                # Message from silicon.cs.umanitoba.ca
                try:
                    res_msg = json.loads(data)

                    command = res_msg['command']

                    if command and command in handle_commands:
                        cmd_func = handle_commands[command]
                        cmd_func(udp_socket, res_msg, addr)

                    if command == 'FLOOD' or command == 'FLOOD-REPLY':
                        print("Sender: ", addr)

                except:
                    pass

            elif source is tcp_socket:
                client_connection, client_addr = tcp_socket.accept()
                tcp_socket.setblocking(False)

                message = client_connection.recv(1024).decode('utf-8')
                msg_argv = message.strip().split(' ')

                cli_response = ''

                if msg_argv[0]:
                    cli_command = msg_argv[0].lower()

                    if cli_command and cli_command in handle_cli_commands:
                        cli_func = handle_cli_commands.get(cli_command)
                        cli_response = cli_func(msg_argv)

                print(cli_response)
                client_connection.sendall(
                    (str(cli_response)+"\r\n").encode('utf-8'))
                client_connection.close()
            else:
                
                # inputs.remove(source)
                pass

except KeyboardInterrupt:
    print('[NODE] Keyboard Interrupt')
    pass
except Exception as e:
    print("[NODE] Program Exception : {}".format(e))
finally:
    print('[NODE] Closing all connections...')
    udp_socket.close()
    tcp_socket.close()
    zeroconf.unregister_service(info)
    zeroconf.close()
