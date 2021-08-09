#!/usr/bin/python3

"""

TODO:

(This part ordered from easy to the most difficult)
5. Implement Consensus

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

NODE_NAME = "E's peer"

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

EVENT_CONSENSUS = 'CONSENSUS'
EVENT_SUB_CONSENSUS = 'SUB_CONSENSUS'

CONSENSUS_TIMEOUT_DUE = 2

tell_truth = True

if (len(sys.argv) > 2):
    print("[ERROR] usage: ./service.py <optional_port>")
    sys.exit()

# ================== UDP Socket for A3 peer-to-peer network ==================
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_socket.setblocking(False)

udp_hostname = socket.gethostname()
# for debugging
udp_hostname = '127.0.0.1'

# This accepts a tuple...
udp_port = 0  # next available

if len(sys.argv) == 2:
    # use port from arguments
    try:
        udp_port = int(sys.argv[1])
    except:
        print("[ERROR] <optional_port> should be a number")
        sys.exit()

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
flood_messages = []
words_db = [''] * 5


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
        "name": "{} on: {}".format(NODE_NAME, socket.gethostname()),
        "messageID": str(uuid.uuid4())
    }

    print(flood_message)

    flood_message = json.dumps(flood_message).encode('utf-8')
    udp_socket.sendto(flood_message, A3_ADDR)
    pass


def parse_peers_addr(list):
    res = []
    for p in list:
        addr = p.split(':')
        res.append((addr[0], int(addr[1])))

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


def cmd_flood(udp_socket, message, addr):
    # global peers
    # global message_queue
    # global flood_messages

    msg_name = message['name']
    msg_host = message['host']
    msg_port = message['port']
    msg_message_id = message['messageID']

    # Bad message
    if not (msg_host and msg_port and msg_name and msg_message_id):
        return None

    # Some Nodes might send back our own FLOOD message
    if NODE_ADDR == (msg_host, msg_port):
        return None

    # Check if we already saw this FLOOD message
    if any(flood_msg['message_id'] == msg_message_id for flood_msg in flood_messages):
        return None

    # New node in the network, add it into our peer list

    last_active = time.time()

    print(message)

    # check if this is a new peer or not
    # if not then set the last active
    if not any([peer['host'] == msg_host and peer['port'] == msg_port
                for peer in peers]):
        print("[DEBUG] ===== FLOOD: NEW PEER =====")
        new_peer = {
            'name': msg_name,
            'host': msg_host,
            'port': msg_port,
            'database': [''] * 5,
            'last_active': last_active
        }

        peers.append(new_peer)
    else:
        for peer in peers:
            if peer['host'] == msg_host and peer['port'] == msg_port:
                peer['last_active'] = time.time()

    new_flood_msg = {
        'message_id': msg_message_id,
        'host': msg_host,
        'port': msg_port,
        'name': msg_name,
        'last_active': last_active,
    }

    flood_messages.append(new_flood_msg)

    # send FLOOD-REPLY to sender
    reply_message = {
        "command": CMD_FLOOD_REPLY_,
        "host": NODE_HOST,
        "port": NODE_PORT,
        "name": "{} on: {}".format(NODE_NAME, socket.gethostname())
    }

    reply_message = json.dumps(reply_message).encode('utf-8')
    udp_socket.sendto(reply_message, (msg_host, msg_port))

    # send new
    for peer in peers:
        if peer['host'] == msg_host and peer['port'] == msg_port:
            continue

        flood_msg = json.dumps(message).encode('utf-8')
        udp_socket.sendto(flood_msg, (peer['host'], peer['port']))

    pass


def cmd_flood_reply(udp_socket, message, addr):
    # global peers
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
            'database': [''] * 5,
            'last_active': time.time()
        }

        # Check if the address is not yourself (malicious messages from other peers, yes I have trust issues)
        # Check if you REALLY never see this peer at all
        if (NODE_ADDR != (host_, port_) and
                not any((host_, port_) == (
                    peer['host'], peer['port']) for peer in peers)
            ):
            print("[DEBUG] ===== FLOOD-REPLY: NEW PEER =====")
            peers.append(peer)
            # print(peers)
    pass


def cmd_consensus(udp_socket, message, addr):
    # print("===== CONSENSUS =====")
    global peers
    global message_queue
    global words_db
    _DEBUG = 0

    _om = message['OM']
    _index = message['index']
    _value = message['value']
    _peers = message['peers']
    _message_id = message['messageID']
    _due = message['due']

    if not (_om and _index and _value and _peers and _message_id and _due):
        # ignore the consensus command
        return None

    if _DEBUG == 1:
        words_db[_index] = _value
        consensus_reply = {
            "command": CMD_CONSENSUS_REPLY_,
            "value": words_db[_index],
            "reply-to": _message_id
        }
        consensus_reply = json.dumps(consensus_reply).encode('utf-8')
        udp_socket.sendto(consensus_reply, addr)
        return None

    # if we are lying, then lie all the time
    if not tell_truth:
        consensus_reply = {
            "command": CMD_CONSENSUS_REPLY_,
            "value": 'LIE',
            "reply-to": _message_id
        }
        consensus_reply = json.dumps(consensus_reply).encode('utf-8')
        udp_socket.sendto(consensus_reply, addr)
        return None

    if _om == 0:
        print("[DEBUG] ========== OM: 0 ==========")
        print(message, addr)
        consensus_reply = {
            "command": CMD_CONSENSUS_REPLY_,
            "value": words_db[_index],
            "reply-to": _message_id
        }
        consensus_reply = json.dumps(consensus_reply).encode('utf-8')
        udp_socket.sendto(consensus_reply, addr)
    elif _om > 0:
        print("[DEBUG] ========== OM > 0 ==========")
        print(message, addr)
        print("DUE in secs: {}".format(_due - time.time()))

        sub_consensus_id = str(uuid.uuid4())
        sub_consensus_due = _due/CONSENSUS_TIMEOUT_DUE

        sub_consensus_msg = {
            "command": CMD_CONSENSUS_,
            "om": _om - 1,
            "index": _index,
            "value": _value,
            "peers": _peers,
            "messageID": sub_consensus_id,
            "due": sub_consensus_due
        }

        sub_consensus_msg = json.dumps(sub_consensus_msg).encode('utf-8')

        consensus_event = {
            "event": EVENT_SUB_CONSENSUS,
            "messageID": sub_consensus_id,
            "index": _index,
            "value": _value,
            "reply_to": _message_id,
            "reply_addr": addr,
            "due": sub_consensus_due,
            "response": dict(),  # value returned from each peer
            "expected_responses": len(_peers) - 2
        }

        message_queue.append(consensus_event)

        # send the message to n-2 peers, everyone except the sender and yourself
        for peer_addr in parse_peers_addr(_peers):
            if peer_addr != NODE_ADDR and peer_addr != addr:
                print(
                    "[DEBUG - CMD] ========== sending SUB-CONSENSUS to: {} =====".format(peer_addr))
                udp_socket.sendto(sub_consensus_msg, peer_addr)

    pass


def cmd_consensus_reply(udp_socket, message, addr):
    global peers
    global message_queue

    _value = str(message['value'])
    _message_id = message['reply-to']

    if not (_value and _message_id):
        # ignore the consensus command
        return None

    consensus = [m for m in message_queue if m['messageID'] == _message_id]

    if not any(consensus):
        return None

    consensus = consensus.pop()

    words_dict = consensus['response']
    if _value in words_dict:
        words_dict[_value] += 1
    else:
        words_dict[_value] = 1

    consensus['expected_responses'] = consensus['expected_responses'] - 1

    print("[DEBUG] - CONSENSUS-REPLY")
    print(consensus, addr)
    pass


def cmd_query(udp_socket, message, addr):
    print(message)
    global words_db
    query_message = {
        "command": CMD_QUERY_
    }
    query_message = json.dumps(query_message).encode('utf-8')
    udp_socket.sendto(query_message, addr)
    pass


def cmd_handle_query(udp_socket, message, addr):
    print(message)
    global words_db
    query_message = {
        "command": CMD_QUERY_REPLY_,
        "database": words_db
    }
    query_message = json.dumps(query_message).encode('utf-8')
    udp_socket.sendto(query_message, addr)
    pass


def cmd_query_reply(udp_socket, message, addr):
    global words_db

    result = message['database']

    if result:
        words_db = result
    pass

# def set_peer_word(word, index, addr):


def cmd_set(udp_socket, message, addr):
    global peers

    index = int(message['index'])
    value = message['value']

    # if not value:
    #     return None

    if index < 0 and index >= 5:
        return None

    print(message, addr)
    words_db[index] = value

    for peer in peers:
        if peer['host'] == addr[0] and peer['port'] == addr[1]:
            peer['database'][index] = value
            break

    pass

# CLI functions


def cli_peers(message):
    global peers
    response = 'Current time: {}\r\n'.format(time.ctime())

    print(peers)

    for peer in peers:
        peer_txt = '{} | {}:{} | {} | {}'.format(
            peer['name'],
            peer['host'],
            peer['port'],
            peer['database'],
            time.ctime(peer['last_active'])
        )

        response += "{}\r\n".format(peer_txt)

    return response if response else "No known peers..."


def cli_current(message):
    return str(words_db)


def cli_consensus(message):
    global message_queue
    global peers

    print(message)

    if(len(message) != 2):
        return 'CONSENSUS usage: consensus <index>'

    index = message[1]

    if not index:
        return None

    try:
        index = int(index)
    except:
        return 'Invalid index for consensus command'

    if int(index) < 0 or int(index) >= 5:
        return 'Invalid index for consensus command'

    om_level = int(len(peers) * (1/3))
    peer_list = ["{}:{}".format(peer['host'], peer['port']) for peer in peers]
    peer_list.append("{}:{}".format(NODE_HOST, NODE_PORT))

    consensus_id = str(uuid.uuid4())
    consensus_due = time.time() + CONSENSUS_TIMEOUT_DUE*15

    consensus_message = {
        "command": CMD_CONSENSUS_REPLY_,
        "om": om_level,
        "index": index,
        "value": words_db[index],
        "peers": peer_list,
        "messageID": consensus_id,
        "due": consensus_due
    }

    consensus_message = json.dumps(consensus_message).encode('utf-8')

    # own consensus have the same message id
    consensus_event = {
        "event": EVENT_SUB_CONSENSUS,
        "messageID": consensus_id,
        "index": index,
        "value": words_db[index],
        "reply_to": consensus_id,
        "reply_addr": NODE_ADDR,
        "due": consensus_due,
        "response": dict(),  # value returned from each peer
        "expected_responses": len(peer_list) - 1
    }

    message_queue.append(consensus_event)

    for peer_addr in parse_peers_addr(peer_list):
        if peer_addr != NODE_ADDR:
            print(
                "[DEBUG - CLI] ========== sending SUB-CONSENSUS to: {} =====".format(peer_addr))
            udp_socket.sendto(consensus_message, peer_addr)

    return "Running consensus on index {}".format(index)


def cli_lie(message):
    global tell_truth
    tell_truth = False
    return "Start lying..."


def cli_truth(message):
    global tell_truth
    tell_truth = True
    return "Stop lying. Telling the truth..."


def cli_set(message):
    global words_db

    if(len(message) != 3):
        return 'SET usage: set <index> <word>'

    index = message[1]
    word = message[2]

    if int(message[1]) < 0 or int(message[1]) >= len(words_db):
        return 'Invalid index for set command'

    words_db[int(message[1])] = message[2]

    set_message = {
        "command": CMD_SET_,
        "index": index,
        "value": word,
    }

    set_message = json.dumps(set_message).encode('utf-8')
    for peer in peers:
        udp_socket.sendto(set_message, (peer['host'], peer['port']))

    return "Done. Set index {} to {}".format(message[1], message[2])


def cli_exit(client_conneciton):
    client_conneciton.sendall(
        ("Server has closed the connection...\r\n").encode('utf-8'))
    client_conneciton.close()
    print("[NODE] Closing CLI connection...")
    return None


def drop_inactive_nodes():

    to_remove = []
    for peer in peers:
        if time.time() - peer['last_active'] > 120:
            to_remove.append(peer)

    for peer in to_remove:
        peers.remove(peer)

    to_remove = []
    for flood in flood_messages:
        if time.time() - flood['last_active'] > 120:
            to_remove.append(flood)

    for flood in to_remove:
        flood_messages.remove(flood)

    pass


def handle_consensus_event(msg):
    global message_queue
    global words_db
    global peers

    _message_id = msg["messageID"]
    _index = msg["index"]
    _value = msg["value"]
    _reply_to = msg["reply_to"]
    _reply_addr = msg["reply_addr"]
    _due = msg["due"]
    _response = msg["response"]
    _expected_responses = msg["expected_responses"]

    current_time = time.time()

    if current_time >= _due or _expected_responses == 0:
        # Over the due date: either reply or set our value
        if _message_id != _reply_to:
            print("[DEBUG] ======== CONSENSUS COMPLETE: Replying to {} ========".format(
                _reply_addr))
            words_db[_index] = most_frequent(_response)
            consensus_reply = {
                "command": CMD_CONSENSUS_REPLY_,
                "value": most_frequent(_response),
                "reply-to": _message_id
            }
            consensus_reply = json.dumps(consensus_reply).encode('utf-8')
            udp_socket.sendto(consensus_reply, _reply_addr)
        else:
            print("[DEBUG] ======== CONSENSUS COMPLETE: Set our DB ========")
            words_db[_index] = most_frequent(_response)

        message_queue.remove(msg)
    pass


handle_commands = dict()
handle_commands[CMD_FLOOD_] = cmd_flood
handle_commands[CMD_FLOOD_REPLY_] = cmd_flood_reply
handle_commands[CMD_CONSENSUS_] = cmd_consensus
handle_commands[CMD_CONSENSUS_REPLY_] = cmd_consensus_reply
handle_commands[CMD_QUERY_] = cmd_handle_query
handle_commands[CMD_QUERY_REPLY_] = cmd_query_reply
handle_commands[CMD_SET_] = cmd_set

handle_cli_commands = dict()
handle_cli_commands[CLI_PEERS_] = cli_peers
handle_cli_commands[CLI_CURRENT_] = cli_current
handle_cli_commands[CLI_CONSENSUS_] = cli_consensus
handle_cli_commands[CLI_LIE_] = cli_lie
handle_cli_commands[CLI_TRUTH_] = cli_truth
handle_cli_commands[CLI_SET_] = cli_set
handle_cli_commands[CLI_EXIT_] = cli_exit


# ================== Register Service ==================
desc = {}

info = ServiceInfo(
    '_magicdice._udp.local.',
    "{}._magicdice._udp.local.".format(NODE_NAME),
    addresses=[socket.inet_aton(socket.gethostbyname(socket.gethostname()))],
    port=external_port,
    properties={},
    server="something_unique.local.",
)

zeroconf = Zeroconf()
zeroconf.register_service(info)


inputs = [udp_socket, tcp_socket]
outputs = []  # None

# join the network and query the database from silicon
# join_network(udp_socket)
# cmd_query(udp_socket, '', A3_ADDR)

last_flood_msg = time.time()
last_drop_inactive_nodes = time.time()

try:
    while True:
        # Need to calculate the timeout in someway...
        # This requirement is pretty BS...
        join_network_timeout_sec = 60 - (time.time() - last_flood_msg)
        join_network_timeout_sec = 0 if join_network_timeout_sec < 0 else join_network_timeout_sec

        drop_nodes_timeout_sec = 120 - (time.time() - last_drop_inactive_nodes)
        drop_nodes_timeout_sec = 0 if drop_nodes_timeout_sec < 0 else drop_nodes_timeout_sec

        timeout_sec = join_network_timeout_sec if join_network_timeout_sec < drop_nodes_timeout_sec else drop_nodes_timeout_sec

        readable, writable, exceptional = select.select(
            inputs, outputs, inputs, join_network_timeout_sec)

        #
        # if timeout_sec <= 0 then it's time to join the network again
        if(time.time() - last_flood_msg > 60):
            join_network(udp_socket)
            last_flood_msg = time.time()

        # drop the inactive nodes
        if(time.time() - last_drop_inactive_nodes > 120):
            drop_inactive_nodes()
            last_drop_inactive_nodes = time.time()

        # handle message queue
        for event in message_queue:
            handle_consensus_event(event)
            pass

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

                    # if command == 'FLOOD' or command == 'FLOOD-REPLY':
                    #     print("Sender: ", addr)

                except:
                    pass

            elif source is tcp_socket:
                # accept CLI client
                client_connection, client_addr = source.accept()
                source.setblocking(False)
                # Add the client socket to the inputs for multiplexing
                inputs.append(client_connection)
            else:
                # handle CLI connection
                message = source.recv(1024).decode('utf-8')
                msg_argv = message.strip().split(' ')

                cli_response = ''

                # check if the message itself exist
                if msg_argv[0]:
                    cli_command = msg_argv[0].lower()

                    if cli_command and cli_command in handle_cli_commands:
                        cli_func = handle_cli_commands.get(cli_command)

                        if cli_command == 'exit':
                            # close client connection and remove from the inputs
                            cli_response = cli_func(source)
                            inputs.remove(source)
                        else:
                            cli_response = cli_func(msg_argv)
                            source.sendall(
                                (str(cli_response)+"\r\n").encode('utf-8'))
                    else:
                        # command does not exist
                        source.sendall(
                            ("Command not recognized\r\n").encode('utf-8'))

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
