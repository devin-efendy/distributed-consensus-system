import socket
import json
import select
import sys
import random
import pprint
import datetime
import copy

# ========================================================
# GLOBAL VARIABLES
# ========================================================

# My Node's Information:
# randomly generate an ID between 1 and (2^16) - 2
MY_ID = random.randint(1, (1 << 16) - 2)
MY_HOST = socket.getfqdn()                  # any "rodents" machine
MY_PORT = 15031                             # my assigned port
MY_ADDRESS = (MY_HOST, MY_PORT)

# Bootstrap's Information:
BOOTSTRAP_ID = (1 << 16)-1
BOOTSTRAP_HOST = "silicon.cs.umanitoba.ca"
BOOTSTRAP_PORT = 14000

# Nodes:
MY_NODE = {"ID": MY_ID, "hostname": MY_HOST, "port": MY_PORT}
BOOTSTRAP_NODE = {"ID": BOOTSTRAP_ID,
                  "hostname": BOOTSTRAP_HOST, "port": BOOTSTRAP_PORT}
PRED_NODE = {"ID": 0, "hostname": BOOTSTRAP_HOST, "port": BOOTSTRAP_PORT}

# States:
TERMINATE_STATE = -1
JOINING_STATE = 1       # first joining
STABILIZING_STATE = 2   # re-joining (stop all request)

dictDHT = {
    "state": JOINING_STATE,
    "self": MY_NODE,  # set at the beginning
    "pred": PRED_NODE,  # set after receiving a setPred
    "succ": BOOTSTRAP_NODE,  # set after joining
    "query": {}
}

# ====================================================================
# JOINING & REJOINING & FIND FUNCTIONS
# ====================================================================

# ------------------------------------------------------------------------
# join()
# This will join in the ring by sending "pred?" message to the BOOTSTRAP.
# It will be called at the beginning of the program, and also when my
# successor's predeccessor is not me.
#
# note:
#   - Assume the bootstrap will always be responding
# ------------------------------------------------------------------------


def join(socket):
    msg = {"cmd": "pred?", "ID": MY_ID, "hostname": MY_HOST, "port": MY_PORT}
    currAddress = (BOOTSTRAP_HOST, BOOTSTRAP_PORT)
    prevNode = BOOTSTRAP_NODE
    isLocationFound = False

    # send initial "pred?"" message to BOOTSTRAP to begin my join
    sendMsg = json.dumps(msg)
    socket.sendto(sendMsg, currAddress)

    while not isLocationFound:
        try:
            (recvMsg, recvAddress) = socket.recvfrom(2048)  # timeout = 2
            recvMsg = json.loads(recvMsg)
            recvMsg = validateMessage(recvMsg)

            isLocationFound = (MY_ID >= recvMsg["thePred"]["ID"])

            # location is found - what to do?
            #   - join into the ring right after the "recvMsg['me']" but before "recvMsg['thePred']",
            #   - set "recvMsg['me']" as my new successor
            #   - send "recvMsg['me']" a "setPred" message
            #   - print out my new successor
            if isLocationFound:
                dictDHT["succ"] = {"ID": recvMsg["me"]["ID"],
                                   "hostname": recvMsg["me"]["hostname"], "port": recvMsg["me"]["port"]}
                printMsg("send", "setPred", {}, dictDHT["succ"])
                msg = {"cmd": "setPred", "ID": MY_ID,
                       "hostname": MY_HOST, "port": MY_PORT}
                currAddress = (recvMsg["me"]["hostname"],
                               recvMsg["me"]["port"])
                sendMsg = json.dumps(msg)
                socket.sendto(sendMsg, currAddress)

            # location is not found - what to do?
            #   - update "prevNode" to be "recvMsg['me']"
            #   - send "recvMsg['thePred']" a "pred?" message (i.e. do recursion)
            else:
                prevNode = {"ID": recvMsg["me"]["ID"], "hostname": recvMsg["me"]
                            ["hostname"], "port": recvMsg["me"]["port"]}
                msg = {"cmd": "pred?", "ID": MY_ID,
                       "hostname": MY_HOST, "port": MY_PORT}
                currAddress = (recvMsg["thePred"]
                               ["hostname"], recvMsg["thePred"]["port"])
                sendMsg = json.dumps(msg)
                socket.sendto(sendMsg, currAddress)

        # except - trigger cases:
        #   (i)     when "socket.recvfrom" has waited for more than two seconds, then exception is thrown to here
        #   (ii)    when the receiving message is having invalid format
        #   (iii)   when the receiving message is "pred?" again from my node in the previous life (i.e. sending msg to myself)
        # except - what to do?
        #   - join into the ring right after the "prevNode",
        #   - set "prevNode" as my new successor
        #   - send "prevNode" a "setPred" message
        #   - print out my new successor
        except:
            isLocationFound = True
            dictDHT["succ"] = {
                "ID": prevNode["ID"], "hostname": prevNode["hostname"], "port": prevNode["port"]}
            printMsg("send", "setPred", {}, dictDHT["succ"])
            msg = {"cmd": "setPred", "ID": MY_ID,
                   "hostname": MY_HOST, "port": MY_PORT}
            currAddress = (prevNode["hostname"], prevNode["port"])
            sendMsg = json.dumps(msg)
            socket.sendto(sendMsg, currAddress)

# ------------------------------------------------------------------------
# rejoin()
# This will check if successor's predeccessor is equal to me or not.
# If it is equal, then do nothing. If it is not equal or an exception pops up,
# then call join() again (In other words, it will join the ring from BOOTSTRAP
# again if a socket timeout exception pops up.). This stabilization function
# will be called every time I need to do a find() query.
# ------------------------------------------------------------------------


def rejoin(socket):

    msg = {"cmd": "pred?", "ID": MY_ID, "hostname": MY_HOST, "port": MY_PORT}
    currAddress = (dictDHT["succ"]["hostname"], dictDHT["succ"]["port"])
    isLocationFound = False
    isSuccPredCorrect = False

    sendMsg = json.dumps(msg)
    socket.sendto(sendMsg, currAddress)

    try:
        (recvMsg, recvAddress) = socket.recvfrom(2048)
        recvMsg = json.loads(recvMsg)
        recvMsg = validateMessage(recvMsg)
        isSuccPredCorrect = compareSuccPredWithMyNode(
            recvMsg["thePred"], MY_NODE)
        if not isSuccPredCorrect:
            join(socket)
    except:
        join(socket)

# ------------------------------------------------------------------------
# find()
# This function will either send a "find" message to my successor, or
# send an "owner" message back to the query sender.
# Before sending the message, two things will be done first:
# one is to validate the query value (should be an integer or a digit string);
# another one is to validate correctness of my successor's predeccessor information,
# by calling rejoin(). While executing this function, requests from any other nodes
# will be ignored.
# ------------------------------------------------------------------------


def find(socket, qMsg):
    dictDHT["state"] = STABILIZING_STATE

    # create a deep copy before modifying it.
    qMsg = copy.deepcopy(qMsg)
    # validate query value
    qVal = validateQueryValue(qMsg["query"], qMsg["ID"])
    # increment hops value
    qHop = qMsg["hops"] + 1
    qMsg["query"] = qVal
    qMsg["hops"] = qHop

    rejoin(socket)

    if MY_ID >= qMsg["query"]:
        msg = {"cmd": "owner", "ID": MY_ID, "hostname": MY_HOST,
               "port": MY_PORT, "hops": qMsg["hops"], "query": qMsg["query"]}
        address = (qMsg["hostname"], qMsg["port"])
        sendMsg = json.dumps(msg)
        socket.sendto(sendMsg, address)
    else:
        msg = qMsg
        address = (dictDHT["succ"]["hostname"], dictDHT["succ"]["port"])
        sendMsg = json.dumps(msg)
        socket.sendto(sendMsg, address)

    dictDHT["state"] = JOINING_STATE


# ====================================================================
# HELPER FUNCTIONS
# ====================================================================
def validateMessage(msg):
    msg = copy.deepcopy(msg)
    if (
        msg.has_key("cmd") and isinstance(msg["cmd"], basestring) and
        msg["cmd"] == "myPred" and
        msg.has_key("me") and msg.has_key("thePred") and
        msg["me"].has_key("ID") and (isinstance(msg["me"]["ID"], int) or (isinstance(msg["me"]["ID"], basestring) and msg["me"]["ID"].isdigit())) and
        msg["me"].has_key("hostname") and isinstance(msg["me"]["hostname"], basestring) and
        msg["me"].has_key("port") and (isinstance(msg["me"]["port"], int) or (isinstance(msg["me"]["port"], basestring) and msg["me"]["port"].isdigit())) and
        msg["thePred"].has_key("ID") and (isinstance(msg["thePred"]["ID"], int) or (isinstance(msg["thePred"]["ID"], basestring) and msg["thePred"]["ID"].isdigit())) and
        msg["thePred"].has_key("hostname") and isinstance(msg["thePred"]["hostname"], basestring) and
        msg["thePred"].has_key("port") and (isinstance(msg["thePred"]["port"], int) or (
            isinstance(msg["thePred"]["port"], basestring) and msg["thePred"]["port"].isdigit()))
    ):
        me = {"ID": int(msg["me"]["ID"]), "hostname": msg["me"]
              ["hostname"], "port": int(msg["me"]["port"])}
        thePred = {"ID": int(msg["thePred"]["ID"]), "hostname": msg["thePred"]
                   ["hostname"], "port": int(msg["thePred"]["port"])}
        msg = {"cmd": msg["cmd"], "me": me, "thePred": thePred}
        return msg

    elif (
        msg.has_key("cmd") and isinstance(msg["cmd"], basestring) and
        (msg["cmd"] == "find" or msg["cmd"] == "owner") and
        msg.has_key("ID") and (isinstance(msg["ID"], int) or (isinstance(msg["ID"], basestring) and msg["ID"].isdigit())) and
        msg.has_key("hostname") and isinstance(msg["hostname"], basestring) and
        msg.has_key("port") and (isinstance(msg["port"], int) or (isinstance(msg["port"], basestring) and msg["port"].isdigit())) and
        msg.has_key("hops") and (isinstance(msg["hops"], int) or (isinstance(msg["hops"], basestring) and msg["hops"].isdigit())) and
        msg.has_key("query") and (isinstance(msg["query"], int) or (
            isinstance(msg["query"], basestring) and msg["query"].isdigit()))
    ):

        msg = {"cmd": msg["cmd"], "ID": int(msg["ID"]), "hostname": msg["hostname"], "port": int(
            msg["port"]), "hops": int(msg["hops"]), "query": int(msg["query"])}
        return msg

    elif (
        msg.has_key("cmd") and isinstance(msg["cmd"], basestring) and
        (msg["cmd"] == "pred?" or msg["cmd"] == "setPred") and
        msg.has_key("ID") and (isinstance(msg["ID"], int) or (isinstance(msg["ID"], basestring) and msg["ID"].isdigit())) and
        msg.has_key("hostname") and isinstance(msg["hostname"], basestring) and
        msg.has_key("port") and (isinstance(msg["port"], int) or (
            isinstance(msg["port"], basestring) and msg["port"].isdigit()))
    ):

        msg = {"cmd": msg["cmd"], "ID": int(
            msg["ID"]), "hostname": msg["hostname"], "port": int(msg["port"])}
        return msg

    else:
        print("[Wrong Message Format Received] {0}".format(msg))
        return ""


def compareSuccPredWithMyNode(succPred, myNode):
    if (
        succPred["ID"] == myNode["ID"] and
        succPred["hostname"] == myNode["hostname"] and
        succPred["port"] == myNode["port"]
    ):
        return True
    else:
        return False


def validateQueryValue(val, intId):
    qVal = val
    qVal_LowerBound = intId + 1
    qVal_UpperBound = ((1 << 16)-1)

    if isinstance(qVal, basestring) and qVal.strip().isdigit():
        qVal = int(qVal)

    if isinstance(qVal, int) and qVal >= qVal_LowerBound and qVal <= qVal_UpperBound:
        return qVal
    else:
        qVal = random.randint(qVal_LowerBound, qVal_UpperBound)
        return qVal


def printMsg(side, cmd, msg, node):
    pp = pprint.PrettyPrinter(indent=1)
    obj = []

    if side == "send" and cmd == "setPred":
        obj = ["======================================================",
               str(datetime.datetime.now()) + " : (re-)joining ring with : ",
               " successor = " + str(node),
               " predecessor = " + str(dictDHT["pred"]),
               "======================================================"]
    elif side == "recv" and cmd == "owner":
        obj = ["======================================================",
               str(datetime.datetime.now()) + " : query value is found! ",
               " owner = " + str(node),
               " qVal = " + str(msg["query"]),
               " # hops = " + str(msg["hops"]),
               "======================================================"]
    elif side == "recv" and cmd == "setPred":
        obj = ["======================================================",
               str(datetime.datetime.now()) +
               " : Node's predecessor has changed to: ",
               " predecessor = " + str(node),
               "======================================================"]
    pp.pprint(obj)

# ====================================================================
# MAIN PROGRAM
# ====================================================================


# create server socket object
print("=================================")
print("[1] MY ID is '{0}'.".format(MY_ID))
print("=================================")
mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
mySocket.bind(MY_ADDRESS)
mySocket.settimeout(2)

# create File Descriptor
socketFD = mySocket.fileno()

# join the ring
join(mySocket)

try:

    while dictDHT["state"] != TERMINATE_STATE:  # I.e. state != 0

        # Heartbeat for concurrent read actions
        (readFDs, writeFDs, errorFDs) = select.select(
            [socketFD, sys.stdin], [], [], 2)

        for desc in readFDs:

            # sys.stdin - cases :
            #   i)      empty string            - terminate the program
            #   ii)     digit string            - call find()
            #   iii)    all other string input  - ignore
            if (desc == sys.stdin):

                # read input
                strVal = sys.stdin.readline().replace('\n', '')  # always string

                # case (i)
                if dictDHT["state"] == JOINING_STATE and strVal == "":
                    dictDHT["state"] = TERMINATE_STATE
                    break

                # case (ii)
                elif dictDHT["state"] == JOINING_STATE and strVal.strip().isdigit():
                    qMsg = {"cmd": "find", "ID": MY_ID, "hostname": MY_HOST,
                            "port": MY_PORT, "hops": -1, "query": int(strVal)}
                    find(mySocket, qMsg)

            # socketFD - cases :
            #   i)      invalidate message          - ignore requests
            #   ii)     STABILIZING_STATE           - ignore requests
            #   iii)    "pred?" & JOINING_STATE     - send "myPred" back
            #   iv)     "setPred" & JOINING_STATE   - update my node's pred
            #   v)      "find" & JOINING_STATE      - call find()
            #   vi)     "owner" & JOINING_STATE     - print the msg out
            elif (desc == socketFD):

                # read socket request message
                (recvMsg, recvAddress) = mySocket.recvfrom(2048)
                recvMsg = json.loads(recvMsg)
                recvMsg = validateMessage(recvMsg)

                if dictDHT["state"] == JOINING_STATE and recvMsg != "":

                    # case (iii)
                    if recvMsg["cmd"] == "pred?":
                        me = {"ID": MY_ID, "hostname": MY_HOST, "port": MY_PORT}
                        thePred = {"ID": dictDHT["pred"]["ID"], "hostname": dictDHT["pred"]
                                   ["hostname"], "port": dictDHT["pred"]["port"]}
                        msg = {"cmd": "myPred", "me": me, "thePred": thePred}
                        sendMsg = json.dumps(msg)
                        mySocket.sendto(sendMsg, recvAddress)

                    # case (iv)
                    elif recvMsg["cmd"] == "setPred":
                        dictDHT["pred"] = {
                            "ID": recvMsg["ID"], "hostname": recvMsg["hostname"], "port": recvMsg["port"]}
                        printMsg("recv", "setPred", recvMsg, dictDHT["pred"])

                    # case (v)
                    elif recvMsg["cmd"] == "find":
                        find(mySocket, recvMsg)

                    # case (vi)
                    elif recvMsg["cmd"] == "owner":
                        ownerNode = {
                            "ID": recvMsg["ID"], "hostname": recvMsg["hostname"], "port": recvMsg["port"]}
                        printMsg("recv", "owner", recvMsg, ownerNode)

                else:
                    print(
                        "[Ensure recvMsg == ""] recvMsg {0}: ".format(recvMsg))

except Exception as e:
    print("Program Exception : {0}".format(e))

finally:
    print("End of Program...")
    mySocket.close()
