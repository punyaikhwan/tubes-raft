import json
import requests
import grequests
import urllib2
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

#baca daftar server dari file txt dan memasukkannya ke list
listServer = [line.rstrip('\n') for line in open('listServer.txt')]

class Node(BaseHTTPRequestHandler):
    def __init__(self, node_id, timeout = 1, list_worker = []):
    # create objek
        self.node_id = node_id # sekaligus jadi port nya
        self.timeout = timeout
        self.list_worker = list_worker
        self.list_workerLoad = [0] * len(list_worker)

    def initialize(self, leader_id, list_nodeID = []):
    # node pertama kali dinyalakan
        self.leader_id = leader_id
        self.list_nodeID = list_nodeID
        self.list_isNodeAlive = [True] * len(list_nodeID)
        self.isAlive = True
        self.isCandidate = False
        self.isAlreadyVoted = False
        self.firstRequestToServer()
        self.nodeMain()

    def firstRequestToServer(self) {
    #mengirimkan request pertama kali ke server agar server mengirimkan
    #informasi ke node
        for server in listServer :
            content = urllib2.urlopen(server).read()
            print content
    }

    def nodeMain(self):
    # program utama node, memilih peran sebagai apa
        while(True):
            if (self.leader_id == self.node_id):
                self.leaderMain()
            elif (self.isCandidate):
                self.candidateMain()
            else:
                self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        while (True):
            if (self.isAlive):
                # UNDER CONSTRUCTION, konsep doang ini
                self.sendHeartbeat()

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        self.isAlreadyVoted = False
        while (True):
            if (self.isAlive):
                # UNDER CONSTRUCTION, konsep doang ini
                timeout = False
                if (timeout):
                    self.isCandidate = True
                    break
                else:
                    self.sendHeartbeatResponse()

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        if (self.sendVoteRequest()):
            self.leader_id = self.node_id
            self.isCandidate = False

    def broadcast(self, command, data =''):
        # persiapkan node tujuan
        list_dest = []
        for nodeID in list_nodeID:
            if (nodeID != self.node_id):
                url = "http://localhost:" + nodeID + command
                list_dest.append(url)
        # kirim
        job = (grequests.post(dest, data = data) for dest in list_dest)
        return grequests.map(job)

    def sendResponse(self, code, data = ''):
        self.send_response(code)
        self.end_headers()
        self.wfile.write(str(data).encode('utf-8'))

    def do_POST(self):
    # menghandle request HTTP POST yang masuk ke node ini (dari node lain)
        try:
            args = self.path.split('/')
            # mengecek command apa yang dikirim node lain
            if (len(args) == 2):
                command = args[1]
                if (command == 'heartbeat'): # dari leader
                    self.sendHeartbeatResponse()
                elif (command == 'vote'): # dari candidate
                    self.sendVoteResponse()
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error

    def do_Get(self):
    # menghandle request HTTP GET yang masuk ke node ini (dari client)
        try:
            args = self.path.split('/')
            # mengecek command apa yang diminta client
            if (len(args) == 2):
                command = args[1]
                if (command == 'pause'):
                    self.pause()
                elif (command == 'resume'):
                    self.resume()
                else:
                    self.sendResponse(400) # Bad Request
            elif (len(args) == 3):
                command = args[1]
                n = int(args[2])
                if (command == 'prime'):
                    self.sendPrimeRequest(n)
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error

    def sendHeartbeat(self):
    # mengirim heartbeat, dilakukan oleh leader
        # persiapkan data dan kirim
        data = json.dumps(self.list_workerLoad)
        responses = self.broadcast('/heartbeat', data)
        # proses responses
        majorityAlive = [0] * len(list_worker)
        for response in responses:
            list_isWorkerAlive = json.loads(response)

    def sendHeartbeatResponse(self):
    # mengirim heartbeat response, dilakukan oleh follower/candidate lain
        # membaca pesan pada heartbeat
        # rawdata = self.rfile.read(int(self.headers.getheader('Content-Length')))
        return

    def sendVoteRequest(self):
    # mengirim vote, dilakukan oleh candidate
        # kirim
        responses = self.broadcast('/vote')
        # proses responses
        voteCount = 0
        for response in responses:
            vote = response.content
            if (vote == 'OK'):
                voteCount += 1
        # hasil apakah terpilih mayoritas
        return (voteCount >= (len(list_node) / 2 + 1))

    def sendVoteResponse(self):
    # mengirim vote response, dilakukan oleh follower
        if (self.isAlreadyVoted):
            self.sendResponse(200, 'NO')
        else:
            self.sendResponse(200, 'OK')
            self.isAlreadyVoted = True

    def pause(self):
    # simulasi ketika node mati
        if (self.isAlive):
            isAlive = False
            self.sendResponse(200, 'Node ' + self.node_id + ' now down.')
        else:
            self.sendResponse(200, 'Node ' + self.node_id + ' already down.')

    def resume(self):
    # simulasi ketika node hidup kembali
        if (self.isAlive):
            self.sendResponse(200, 'Node ' + self.node_id + ' already up.')
        else:
            isAlive = True
            self.sendResponse(200, 'Node ' + self.node_id + ' now up.')

    def sendPrimeRequest(self, n):
    # meminta bilangan prima ke worker, dilakukan oleh leader/follower
        return

# KONSTANTA
maxload = 1000 # jika worker mati maka elemen ybs pada list_workerLoad = maxload

# VARIABLE GLOBAL
list_worker = []
list_node = []
list_nodeID = []
n_node = 5 # hardcode

# PROGRAM UTAMA
for i in range(n_node):
    node = Node(i, i, list_worker)
    list_node.append(node)
    list_nodeID.append(i)

for node in list_node:
    # penentuan leader pertama kali di hardcode
    node.initialize(1, list_nodeID)