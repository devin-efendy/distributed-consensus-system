import socket
import json
import unittest
import uuid
import time

# this is your server ip/port.
# best tested locally, without the rest of the network attached
# To test on aviary, change this to the ip of the bird you're using
# or make sure you're binding to '' (empty string)
SERVER = ('127.0.0.1', 16000)

ARRAY_SIZE = 5

class TestAPI(unittest.TestCase):

    def setUp(self):
        '''
        make a new peer for every test
        '''
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((socket.gethostname(), 0))
        self.sock.settimeout(1)


    def test_flood(self):
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock2.bind((socket.gethostname(), 0))
        sock2.settimeout(1)

        
        # log in the first
        obj = {'command': 'FLOOD',
            'host': self.sock.getsockname()[0],
            'port': self.sock.getsockname()[1],
            'name': "1",
            'messageID': str(uuid.uuid4())}
        msg = json.dumps(obj).encode()
        self.sock.sendto(msg, SERVER)
        # listen for a result
        data, addr = self.sock.recvfrom(1024)

        # should tell us about itself
        resultObj = json.loads(data)
        self.assertEqual(resultObj['command'], 'FLOOD-REPLY')
        self.assertEqual(resultObj['host'], SERVER[0])
        self.assertEqual(resultObj['port'], SERVER[1])

        # log in the second
        # should tell us about itself
        # and forward it to the first, too
        # log in the first
        msgID = str(uuid.uuid4())
        obj = {'command': 'FLOOD',
            'host': sock2.getsockname()[0],
            'port': sock2.getsockname()[1],
            'name': "2",
            'messageID': msgID}
        msg = json.dumps(obj).encode()
        sock2.sendto(msg, SERVER)

        # listen for a result
        # should tell us about itself
        # Might not be the first message
        found = False
        while True:
            try:
                data, addr = sock2.recvfrom(1024)
                resultObj = json.loads(data)
                print(resultObj)
                if resultObj['command'] == 'FLOOD-REPLY':
                    found = True
                    break
            except socket.timeout:
                # No more messages
                break
        self.assertTrue(found)
        self.assertEqual(resultObj['command'], 'FLOOD-REPLY')
        self.assertEqual(resultObj['host'], SERVER[0])
        self.assertEqual(resultObj['port'], SERVER[1])

        # The forward
        # listen back on the original to hear sock2's message
        # ... there could be many messages, it just
        # has to be one of them.
        found = False
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                resultObj = json.loads(data)
                if resultObj['command'] == 'FLOOD' and resultObj['messageID'] == msgID:
                    found = True
                    break
            except socket.timeout:
                # No more messages
                self.fail("Did not hear the flood forward")
                break
            
            resultObj = json.loads(data)
        self.assertEqual(resultObj['command'], 'FLOOD')
        self.assertTrue(found)

        sock2.close()
    
    def test_set(self):
        # try setting the values for the client
        setTo = []
        for i in range(ARRAY_SIZE):
            setTo.append(i*10)
            obj = { 'command': 'SET', 'index': i, 'value': i*10 }
            msg = json.dumps(obj).encode()
            self.sock.sendto(msg, SERVER)

        # ... are they set?
        obj = { 'command': 'QUERY'}
        msg = json.dumps(obj).encode()
        self.sock.sendto(msg, SERVER)
        data, addr = self.sock.recvfrom(1024)

        obj = json.loads(data.decode('utf-8'))
        self.assertEqual(setTo, obj['database'])

    def test_consensus(self):
        '''
        start more 3 fake 'peers'.
        Then, from the main socket (self.sock) send a consensus command
        with om(2)
        Set the value before we start, then send a different one from all
        three 'generals'. It should send back our differnt workd.
        '''

        oldWord = "somethning"
        newWord = "differnt"

        # set 
        obj = { 'command': 'SET', 'index': 0, 'value': oldWord}
        msg = json.dumps(obj).encode()
        self.sock.sendto(msg, SERVER)

        # set up 3 sockets
        # make the peer list while we're iterating
        generals = []
        peerList = []
        for _ in range(3):
            g = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            g.bind((socket.gethostname(), 0))
            g.settimeout(1)
            generals.append(g)
            info = g.getsockname()
            peerList.append("{}:{}".format(info[0], info[1]))

        # send the command
        msgID = str(uuid.uuid4())
        obj = {
            "command": "CONSENSUS",
            "OM": 1,
            "peers": peerList,
            "messageID": msgID,
            "index": 0,
            "value": oldWord,
            "due": time.time() + 1 # use an int to be a little more safe
        }
        msg = json.dumps(obj).encode()
        self.sock.sendto(msg, SERVER)

        # wait... just a bit
        time.sleep(0.1)

        # ok, now listen on all the generals
        for g in generals:
            data, addr = g.recvfrom(1024)
            obj = json.loads(data.decode('utf-8'))

            # should have om 0
            self.assertEqual(obj['OM'], 0)
            # a different message ID
            self.assertNotEqual(msgID, obj['messageID'])
            # the right command...
            self.assertEqual('CONSENSUS', obj['command'])
            
            # We can just reply
            reply = {
                "command": "CONSENSUS-REPLY",
                "value": newWord,
                "reply-to": obj['messageID']
            }
            replyB = json.dumps(reply).encode()
            g.sendto(replyB, addr)
        
        # main socket should have a reply now
        # wait... just a bit (but longer than the timeout)
        time.sleep(2)

        try:
            data, addr = self.sock.recvfrom(1024)
            obj = json.loads(data.decode('utf-8'))
        except:
            self.fail("Problem getting data from socket")

        # got the right word back?
        self.assertEqual(obj['value'], newWord)
        # with the right id?
        self.assertEqual(obj['reply-to'], msgID)

        for g in generals:
            g.close()


    # def test_consensus_no_reply(self):
    #     '''
    #     Test that a consensus that has no repliers will
    #     still send a reply
    #     '''

    #     oldWord = "somethning"
    #     newWord = "differnt"

    #     # set 
    #     obj = { 'command': 'SET', 'index': 0, 'value': oldWord}
    #     msg = json.dumps(obj).encode()
    #     self.sock.sendto(msg, SERVER)

       
    #     # send the command
    #     msgID = str(uuid.uuid4())
    #     obj = {
    #         "command": "CONSENSUS",
    #         "OM": 1,
    #         "peers": [
    #             '127.0.0.1:55555',
    #             '127.0.0.1:55556',
    #             '127.0.0.1:55557',
    #             '127.0.0.1:16000'],  # the test machine
    #         "messageID": msgID,
    #         "index": 0,
    #         "value": oldWord,
    #         "due": time.time() + 0.5
    #     }
    #     msg = json.dumps(obj).encode()
    #     self.sock.sendto(msg, SERVER)
        
    #     # wait... just a bit more than the due date
    #     time.sleep(0.6)

    #     data, addr = data, addr = self.sock.recvfrom(1024)
    #     obj = json.loads(data.decode('utf-8'))

    #     # got the right word back?
    #     self.assertEqual(obj['command'], 'CONSENSUS-REPLY')
    #     self.assertEqual(obj['value'], oldWord)
    #     # with the right id?
    #     self.assertEqual(obj['reply-to'], msgID)



    def tearDown(self):
        self.sock.close()
        obj = { 'command': 'QUERY'}

if __name__ == '__main__':
    unittest.main(verbosity=2)