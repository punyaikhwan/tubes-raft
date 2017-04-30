import grequests
import json
import requests
import sys
import thread
import time
import urllib2
from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer

################################################################################

class NodeHandler(BaseHTTPRequestHandler):
    def getContent(self):
    # mengambil data yang terkandung dalam content HTTP request
        contentLength = int(self.headers.getheader('content-length', 0))
        return self.rfile.read(contentLength)

    def sendResponse(self, code, data = ''):
    # mengirim response dari HTTP request.
    # bisa dipakai oleh prosedur lain, karena code dan data content bisa custom
        self.send_response(code)
        self.end_headers()
        self.wfile.write(str(data).encode('utf-8'))

    def sendHeartbeatResponse(self):
    # format content heartbeat:
    # JSON [senderAddress, [listWorkerLoadSender]]
        global address
        global isAlive
        global leaderAddress
        global listWorkerLoadLeader
        global roundElection
        if ((not isAlive) or roundElection > 0): # lagi di-pause atau lagi di masa pemilihan leader
            return
        print 'Reading heartbeat...'
        content = json.loads(self.getContent())
        if (content[0] != leaderAddress): # heartbeat bukan dari leader sekarang
            if (leaderAddress == address): # merasa dirinya masih menjadi leader
                leaderAddress = content[0]
            else:
                return
        listWorkerLoadLeader = content[1] # copy data dari leader
        print 'Sending heartbeat response...'
        data = [address, json.dumps(listWorkerLoad)]
        self.sendResponse(200, data)

    def sendVoteResponse(self):
    # format content vote:
    # senderAddress
        global isAlive
        global isAlreadyVoted
        global roundElection
        if (not isAlive): # lagi di-pause
            return
        print 'Vote request received from ', self.getContent()
        if (isAlreadyVoted): # gabisa vote lebih dari sekali
            print 'Sending vote response: NO'
            self.sendResponse(200, 'NO')
        else:
            print 'Sending vote response: OK'
            self.sendResponse(200, 'OK')
            isAlreadyVoted = True
            roundElection += 1

    def processHeartbeatServer(self):
    # format content heartbeat server:
    # JSON [senderAddress, senderLoad]
        global isAlive
        global listWorkerAddress
        global listWorkerLoad
        if (not isAlive):
            return # lagi di-pause
        print 'Reading workload info...'
        content = json.loads(self.getContent())
        idxWorkerAddress = self.listWorkerAddress.index(content[0])
        listWorkerLoad[idxWorkerAddress] = content[1]
        print 'Workload info', content[0], 'updated.'

    def processElectionResult(self):
    # format content election result
    # JSON [senderAddress, 'WIN'] atau [senderAddress, 'LOSE']
        global isAlive
        global isAlreadyVoted
        global isCandidate
        global leaderAddress
        global roundElection
        if (not isAlive):
            return # lagi di-pause
        content = json.loads(self.getContent())
        if (content[1] == 'WIN'): # candidate menang, ketua baru
            leaderAddress = content[0]
            roundElection = 0
            isCandidate = False
        else:
            isCandidate = True
            roundElection += 1
        isAlreadyVoted = False

    def pause(self):
        global isAlive
        global isCandidate
        if (isCandidate):
            self.sendResponse(200, 'Can not shut down at this time.')
        elif (isAlive):
            print 'Node now down.'
            isAlive = False
            self.sendResponse(200, 'Node now down.')
        else:
            self.sendResponse(200, 'Node already down.')

    def resume(self):
        global isAlive
        if (isAlive):
            self.sendResponse(200, 'Node already up.')
        else:
            print 'Node now up.'
            isAlive = True
            self.sendResponse(200, 'Node now up.')

    def sendPrimeRequest(self, n):
        global isAlive
        global listWorkerAddress
        global listWorkerLoadLeader
        global roundElection
        if ((not isAlive) or roundElection > 0): # lagi di-pause atau lagi di masa pemilihan leader
            return
        print 'Requesting prime number to worker...'
        idxWorkerAddress = listWorkerLoadLeader.index(min(listWorkerLoadLeader)) # cari worker dengan load terkecil
        workerAddress = listWorkerAddress[idxWorkerAddress] + '/' + n
        prime = str(urllib2.urlopen(workerAddress).read()) # kirim request
        print 'Forward prime number response: ', prime
        self.sendResponse(200, prime)

    def do_POST(self):
    # method POST hanya di-request oleh node lain
        try:
            args = self.path.split('/') # mengambil command yang dikirim
            if (len(args) == 2):
                command = args[1]
                if (command == 'heartbeat'): # dari leader
                    self.sendHeartbeatResponse()
                elif (command == 'vote'): # dari candidate
                    self.sendVoteResponse()
                elif (command == 'server'): # dari server
                    self.processHeartbeatServer()
                elif (command == 'election'): # dari candidate
                    self.processElectionResult()
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error

    def do_GET(self):
    # method GET hanya di-request oleh client (melalui browser)
        try:
            args = self.path.split('/') # mengambil command yang diminta
            if (len(args) == 2):
                command = str(args[1])
                if (command == 'pause'):
                    self.pause()
                elif (command == 'resume'):
                    self.resume()
                else:
                    self.sendResponse(400) # Bad Request
            elif (len(args) == 3):
                command = str(args[1])
                n = str(args[2])
                if (command == 'prime' and isAlive):
                    self.sendPrimeRequest(n)
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error
            print ex

################################################################################

class Node():
    def __init__(self):
        global address
        print 'Starting... '
        portNode = address.split(':')[2] # mengambil port
        self.handler = HTTPServer(("", int(portNode)), NodeHandler)
        thread.start_new_thread(self.handler.serve_forever, ()) # nyalakan handler

    def run(self):
    # node berganti peran antara leader, candidate, atau follower
        global address
        global isCandidate
        global leaderAddress
        while(True):
            print 'Determining job...'
            if (leaderAddress == address):
                self.leaderMain()
            elif (isCandidate):
                self.candidateMain()
            else:
                self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        global address
        global isAlive
        global leaderAddress
        print 'I am a leader!'
        while ((leaderAddress == address) and isAlive):
            self.sendHeartbeat()

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        global address
        global isCandidate
        global leaderAddress
        global timeout
        print 'I am a candidate!'
        if (self.sendVoteRequest()): # minta vote, apakah mayoritas memilih dia
            isCandidate = False
            leaderAddress = address # terpilih jadi leader
            data = json.dumps(address, 'WIN')
            self.broadcastToOtherNodes('/election', data)
        else:
            data = json.dumps(address, 'LOSE')
            self.broadcastToOtherNodes('/election', data)
            time.sleep(timeout)

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        global isAlive
        global isCandidate
        global roundElection
        global timeoutCountdown
        print 'I am a follower.'
        while ((roundElection == 0) and isAlive):
            if (timeoutCountdown <= 0):
                isCandidate = True
                roundElection += 1

    def broadcastToOtherNodes(self, command, data = ''):
        global address
        global listNodeAddress
        listDestinationAddress = [] # node selain node sendiri
        for nodeAddress in listNodeAddress:
            if (nodeAddress != address):
                listDestinationAddress.append(nodeAddress + command)
        job = (grequests.post(destinationAddress, data = data) for destinationAddress in listDestinationAddress)
        return grequests.map(job) # kirim

    def sendHeartbeat(self):
        global listWorkerAddress
        global listWorkerLoad
        print 'Sending heartbeat...'
        data = [address, listWorkerLoad]
        data = json.dumps(data)
        listResponse = self.broadcastToOtherNodes('/heartbeat', data)
        print 'Counting majority...'
        # format content heartbeat response:
        # JSON [senderAddress, [listWorkerLoadSender]]
        # UNDER CONSTRUCTION

    def sendVoteRequest(self):
        global address
        print 'Requesting vote...'
        listResponse = self.broadcastToOtherNodes('/vote', address)
        # format content vote response:
        # 'OK' or 'NO'
        voteCount = 0
        for response in listResponse:
            if (response.content == 'OK'):
                voteCount += 1
        return (voteCount >= (len(listNodeAddress) / 2 + 1)) # hasil apakah terpilih mayoritas

################################################################################

# KONSTANTA
maxload = 1000 # jika worker mati maka elemen ybs pada listWorkerLoad = maxload

# GLOBAL VARIABLE, DIPAKE DI Node DAN NodeHandler
address = ''
isAlive = True
isAlreadyVoted = False
isCandidate = False
leaderAddress = ''
listNodeAddress =[line.rstrip('\n') for line in open('listNodeAddress.txt')]
listWorkerAddress = [line.rstrip('\n') for line in open('listWorkerAddress.txt')]
listWorkerLoad = [maxload] * len(listWorkerAddress)
listWorkerLoadLeader = []
roundElection = 0
timeout = 1
timeoutCountdown = timeout

# PROGRAM UTAMA
# parsing data untuk mengetahui node-node yang terdaftar
for i in range(len(listNodeAddress)):
    nodeAddress = listNodeAddress[i].split(' ')
    if (nodeAddress[1] == 'leader'):
        leaderAddress = nodeAddress[0]
    listNodeAddress[i] = nodeAddress[0]
# tentukan port node dari terminal
if (len(sys.argv) == 3):
    address = 'http://' + sys.argv[1]
    # cek apakah IP:port ada di list
    if (address in listNodeAddress):
        timeout = sys.argv[2]
        timeoutCountdown = timeout
        Node().run()
    else:
        print 'address(es) not listed in listNodeAddress.txt'
else:
    print 'usage: node.py <IP:port> <timeout>'
