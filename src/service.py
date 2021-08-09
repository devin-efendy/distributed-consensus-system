#!/usr/bin/python3

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

USE_LOCALHOST = True

tell_truth = True

if (len(sys.argv) > 2):
    print("[ERROR] usage: ./service.py <optional_port>")
    sys.exit()

argv_udp_port = 0  # next available

if len(sys.argv) == 2:
    # use port from arguments
    try:
        argv_udp_port = int(sys.argv[1])
    except:
        print("[ERROR] <optional_port> should be a number")
        sys.exit()

# ================== UDP Socket for A3 peer-to-peer network ==================
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_socket.setblocking(False)

udp_hostname = socket.gethostname()
# for debugging
if USE_LOCALHOST:
    udp_hostname = '127.0.0.1'

# This accepts a tuple...
udp_port = argv_udp_port
udp_socket.bind((udp_hostname, udp_port))

ip_addr = udp_socket.getsockname()[0]
external_port = udp_socket.getsockname()[1]

print("[UDP] Listening on interface " + udp_hostname +
      " port " + str(external_port) + " aka " + ip_addr)

NODE_HOST = socket.gethostbyname(socket.gethostname())
if USE_LOCALHOST:
    NODE_HOST = '127.0.0.1'
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
event_global_timeout = sys.maxsize


def is_node_inactive(node):
    return time.time() - node['last_active'] >= 120


def join_network(udp_socket):
    """
    FLOOD message template
    use udp_socket.sendto(message, address) to send the first FLOOD message
    Then, join the networks
    """
    flood_message = json.dumps({
        "command": CMD_FLOOD_,
        "host": NODE_HOST,
        "port": NODE_PORT,
        "name": "{} on: {}".format(NODE_NAME, socket.gethostname()),
        "messageID": str(uuid.uuid4())
    }).encode('utf-8')

    if not peers:
        udp_socket.sendto(flood_message, A3_ADDR)
    else:
        for p in peers:
            udp_socket.sendto(flood_message, (p['host'], p['port']))
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

    # check if this is a new peer or not
    # if not then set the last active
    if not any([peer['host'] == msg_host and peer['port'] == msg_port
                for peer in peers]):
        # print("[DEBUG] ===== FLOOD: NEW PEER =====")
        peers.append({
            'name': msg_name,
            'host': msg_host,
            'port': msg_port,
            'database': [''] * 5,
            'last_active': last_active
        })
    else:
        for peer in peers:
            if peer['host'] == msg_host and peer['port'] == msg_port:
                peer['last_active'] = time.time()

    flood_messages.append({
        'message_id': msg_message_id,
        'host': msg_host,
        'port': msg_port,
        'name': msg_name,
        'last_active': last_active,
    })

    # send FLOOD-REPLY to sender
    reply_message = json.dumps({
        "command": CMD_FLOOD_REPLY_,
        "host": NODE_HOST,
        "port": NODE_PORT,
        "name": "{} on: {}".format(NODE_NAME, socket.gethostname())
    }).encode('utf-8')
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

    # Input ERROR check
    # Make sure OM and Index are integer
    if not isinstance(_om, int) and not isinstance(_index, int):
        return None
    # Make sure index not out of bound
    if _index < 0 or _index > 4:
        return None
    # Make sure the due date is bigger than the current time
    if _due < time.time():
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
        consensus_reply = json.dumps({
            "command": CMD_CONSENSUS_REPLY_,
            "value": 'LIE',
            "reply-to": _message_id
        }).encode('utf-8')
        udp_socket.sendto(consensus_reply, addr)
        return None

    # print(message)

    if _om == 0:
        print("[DEBUG] ========== OM: 0 ==========")
        print(message, addr)
        consensus_reply = json.dumps({
            "command": CMD_CONSENSUS_REPLY_,
            "value": words_db[_index],
            "reply-to": _message_id
        }).encode('utf-8')
        udp_socket.sendto(consensus_reply, addr)
    elif _om > 0:

        sub_consensus_id = str(uuid.uuid4())
        due_in_sec = _due - time.time()
        sub_consensus_due = _due - due_in_sec/CONSENSUS_TIMEOUT_DUE

        print("[DEBUG] ========== OM > 0 ========== {}".format(addr))
        print(message)
        # print("DUE time: {}".format(_due))
        # print("DUE in secs: {}".format(due_in_sec))
        # print("SUB-DUE in secs: {}".format(sub_consensus_due))

        sub_consensus_peers = []

        # remove ourselves from the peer list for the sub-consensus
        for p in _peers:
            p_addr = p.split(':')
            if NODE_ADDR != (p_addr[0], int(p_addr[1])) and addr != (p_addr[0], int(p_addr[1])):
                sub_consensus_peers.append(p)

        # Add back the sender. The check inside for loop is just incase if the sender include itself
        # inside the peer list
        sub_consensus_peers.append("{}:{}".format(addr[0], addr[1]))

        sub_consensus_msg = json.dumps({
            "command": CMD_CONSENSUS_,
            "OM": _om - 1,
            "index": _index,
            "value": _value,
            "peers": sub_consensus_peers,
            "messageID": sub_consensus_id,
            "due": sub_consensus_due
        }).encode('utf-8')

        consensus_event = {
            "event": EVENT_SUB_CONSENSUS,
            "messageID": sub_consensus_id,
            "index": _index,
            "value": _value,
            "reply_to": _message_id,
            "reply_addr": addr,
            "due": sub_consensus_due,
            "response": dict(),  # value returned from each peer
            "expected_responses": len(sub_consensus_peers) - 1
        }

        message_queue.append(consensus_event)

        # send the message to n-2 peers, everyone except the sender and yourself
        for peer_addr in parse_peers_addr(sub_consensus_peers):
            if peer_addr != addr:
                # print("[DEBUG - CMD] ========== sending SUB-CONSENSUS to: {} =====".format(peer_addr))
                udp_socket.sendto(sub_consensus_msg, peer_addr)

        # print("[CONSENSUS] MessageQueue: {}".format(message_queue))
    pass


def cmd_consensus_reply(udp_socket, message, addr):
    global peers
    global message_queue

    if not 'value' in message and not 'reply-to' in message:
        # ignore the consensus command
        return None

    _value = message['value']
    _message_id = message['reply-to']

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
    # print(message)
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


def cmd_set(udp_socket, message, addr):
    global peers

    index = int(message['index'])
    value = message['value']

    # if not value:
    #     return None

    if index < 0 and index >= 5:
        return None

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

    consensus_id = str(uuid.uuid4())
    consensus_due = time.time() + CONSENSUS_TIMEOUT_DUE*15

    consensus_message = {
        "command": CMD_CONSENSUS_REPLY_,
        "OM": om_level,
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
            # print("[DEBUG - CLI] ========== sending SUB-CONSENSUS to: {} =====".format(peer_addr))
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

    set_message = json.dumps({
        "command": CMD_SET_,
        "index": index,
        "value": word,
    }).encode('utf-8')

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
    global event_global_timeout

    print("[DEBUG] ======== HANDLE CONSENSUS EVENT ========")

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

            response_value = _value if not _response else most_frequent(
                _response)

            words_db[_index] = response_value

            consensus_reply = json.dumps({
                "command": CMD_CONSENSUS_REPLY_,
                "value": response_value,
                "reply-to": _reply_to
            }).encode('utf-8')
            udp_socket.sendto(consensus_reply, _reply_addr)
        else:
            print("[DEBUG] ======== CONSENSUS COMPLETE: Set our DB ========")
            words_db[_index] = _value if not _response else most_frequent(
                _response)

        message_queue.remove(msg)
        # Reset the event global timeout, this one is finished
        event_global_timeout = sys.maxsize
    else:
        event_global_timeout = min(event_global_timeout, _due - current_time)
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

if not USE_LOCALHOST:
    # join the network and query the database from silicon
    join_network(udp_socket)
    cmd_query(udp_socket, '', A3_ADDR)

last_flood_msg = time.time()
last_drop_inactive_nodes = time.time()

try:
    while True:
        # handle message queue
        for event in message_queue:
            handle_consensus_event(event)

        join_network_timeout_sec = 60 - (time.time() - last_flood_msg)
        join_network_timeout_sec = 0 if join_network_timeout_sec < 0 else join_network_timeout_sec

        drop_nodes_timeout_sec = 120 - (time.time() - last_drop_inactive_nodes)
        drop_nodes_timeout_sec = 0 if drop_nodes_timeout_sec < 0 else drop_nodes_timeout_sec

        """
        We maintain and track THREE timeouts:
        1. Global event time (inside the message queue) - the minimum due time from all events
        2. Timeout for re-joining the network (FLOOD to silicon) - every 1 minutes
        3. Timeout for dropping all inactive nodes - every 2 minutes

        Find the minimum timeout from the three trackers
        """
        timeout_sec = min(join_network_timeout_sec,
                          drop_nodes_timeout_sec, event_global_timeout)

        readable, writable, exceptional = select.select(
            inputs, outputs, inputs, timeout_sec)

        # if timeout_sec <= 0 then it's time to join the network again
        if(time.time() - last_flood_msg > 60):
            join_network(udp_socket)
            last_flood_msg = time.time()

        # drop the inactive nodes
        if(time.time() - last_drop_inactive_nodes > 120):
            drop_inactive_nodes()
            last_drop_inactive_nodes = time.time()

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
