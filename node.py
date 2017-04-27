import json
import requests
import urllib2
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

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
        # jalankan peran leader/follower
        if (self.leader_id == self.node_id):
            self.leaderMain()
        else:
            self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        while (True):
            if (self.isAlive):
                return

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        while (True):
            if (self.isAlive):
                return

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        while (True):
            if (self.isAlive):
                return

    def send_POST(self):
        return

    def sendResponse(self, code = 200, data = ''):
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
            elif (len(args) == 3):
                command = args[1]
                n = int(args[2])
                if (command == 'prime'):
                    self.sendPrimeRequest(n)
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error

    def sendHeartbeat(self):
    # mengirim heartbeat, dilakukan oleh leader
        # kirim heartbeat, kumpulkan respon nya
        list_response = []
        for nodeID in list_nodeID:
            if (nodeID != self.node_id):
                url = "http://localhost:" + nodeID + "/heartbeat"
                req = requests.post(url, data = json.dumps(self.list_workerLoad))
                list_response.append(req.text())

    def sendHeartbeatResponse(self):
    # mengirim heartbeat response, dilakukan oleh follower/candidate lain
        # membaca pesan pada heartbeat
        rawdata = self.rfile.read(int(self.headers.getheader('Content-Length')))

    def sendVoteRequest(self):
    # mengirim vote, dilakukan oleh candidate
        return

    def sendVoteResponse(self):
    # mengirim vote response, dilakukan oleh follower
        self.sendResponse(200, 'OK')

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