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

    def sendResponse(self, data = None):
        self.send_response(200) # 200 = OK
        self.end_headers()
        self.wfile.write(str(data).encode('utf-8'))

    def do_Get(self, data = None, list_dest = []):
    # menghandle request HTTP GET yang masuk ke node ini
        try:
            args = self.path.split('/')
            # mengecek siapa yang mengirim request
            if (len(args) == 2):
                command = args[1]
                if (command == 'heartbeat'): # dari leader
                    self.sendHeartbeatResponse()
                elif (command == 'vote'): # dari candidate
                    self.sendVoteResponse()
                elif (command == 'pause'):
                    self.pause()
                elif (command == 'resume'):
                    self.resume()
            elif (len(args) == 3):
                command = args[1]
                n = int(args[2])
                if (command == 'prime'): # dari client
                    self.sendPrimeRequest(n)
            else:
                raise Exception()
        except Exception as ex:
            self.send_response(500) # 500 = Internal Server Error
            self.end_headers()

    def sendHeartbeat(self):
    # mengirim heartbeat, dilakukan oleh leader
        for nodeID in list_nodeID:
            if (nodeID != self.node_id):
                urllib2.urlopen("http://localhost:" + nodeID + "/heartbeat")

    def sendHeartbeatResponse(self):
    # mengirim heartbeat response, dilakukan oleh follower/candidate lain
        # membaca pesan pada heartbeat
        rawdata = self.rfile.read(int(self.headers.getheader('Content-Length')))

    def sendVoteRequest(self):
    # mengirim vote, dilakukan oleh candidate
        return

    def sendVoteResponse(self):
    # mengirim vote response, dilakukan oleh follower
        # membaca pesan pada vote request
        rawdata = self.rfile.read(int(self.headers.getheader('Content-Length')))

    def pause(self):
    # simulasi ketika node mati
        if (self.isAlive):
            isAlive = False
            self.sendResponse('Node ' + self.node_id + ' now down.')
        else:
            self.sendResponse('Node ' + self.node_id + ' already down.')

    def resume(self):
    # simulasi ketika node hidup kembali
        if (self.isAlive):
            self.sendResponse('Node ' + self.node_id + ' already up.')
        else:
            isAlive = True
            self.sendResponse('Node ' + self.node_id + ' now up.')

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