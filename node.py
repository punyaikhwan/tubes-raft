from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

class Node():
    def __init__(self, node_id, timeout = 1, list_worker = []):
    # create objek
        self.node_id = node_id
        self.timeout = timeout
        self.list_worker = list_worker
        self.list_workerLoad = [0] * len(list_worker)

    def initialize(self, leader_id, list_node = []):
    # node pertama kali dinyalakan
        self.leader_id = leader_id
        self.list_node = list_node
        self.list_isNodeAlive = [True] * len(list_node)
        # jalankan peran leader/follower
        if (self.leader_id == self.node_id):
            self.leaderMain()
        else:
            self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        return

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        return

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        return

    def doGet(self, data = None, list_dest = []):
    # mengirim request/response
        return

    def sendHeartbeat(self):
    # mengirim heartbeat, dilakukan oleh leader
        return

    def sendHeartbeatResponse(self):
    # mengirim heartbeat response, dilakukan oleh follower/candidate lain
        return

    def sendVoteRequest(self):
    # mengirim vote, dilakukan oleh candidate
        return

    def sendVoteResponse(self):
    # mengirim vote response, dilakukan oleh follower
        return

    def pause(self):
    # simulasi ketika node mati
        return

    def resume(self):
    # simulasi ketika node hidup kembali
        return

    def sendPrimeRequest(self):
    # meminta bilangan prima ke worker, dilakukan oleh leader/follower
        return

# KONSTANTA
maxload = 1000 # jika worker mati maka elemen ybs pada list_workerLoad = maxload

# VARIABLE GLOBAL
list_worker = []
list_node = []
n_node = 5 # hardcode

# PROGRAM UTAMA
for i in range(n_node):
    node = Node(i, i, list_worker)
    list_node.append(node)

for i in range(n_node):
    # penentuan leader pertama kali di hardcode
    node.initialize(1, list_node)