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
        global leaderAddress
        global listWorkerLoadLeader
        if ((not isAlive) or roundElection > 0): # lagi di-pause atau lagi di masa pemilihan leader
            return
        restoreCountdown()
        content = json.loads(self.getContent())
        print 'Heartbeat received from', content[0], ':', content[1]
        if (content[0] != leaderAddress): # heartbeat bukan dari leader sekarang
            if (address == leaderAddress): # merasa dirinya masih menjadi leader
                leaderAddress = content[0]
            else:
                return
        listWorkerLoadLeader = content[1] # copy data dari leader
        data = [address, json.dumps(listWorkerLoad)]
        print 'Sending heartbeat response:', data[1]
        self.sendResponse(200, data)

    def sendVoteResponse(self):
    # format content vote:
    # senderAddress
        global isAlreadyVoted
        global leaderAddress
        global roundElection
        if (not isAlive): # lagi di-pause
            return
        if (address == leaderAddress):
            leaderAddress = ''
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
        global listWorkerLoad
        if (not isAlive):
            return # lagi di-pause
        content = json.loads(self.getContent())
        idxWorkerAddress = self.listWorkerAddress.index(content[0])
        listWorkerLoad[idxWorkerAddress] = content[1]
        print 'Workload info', content[0], 'updated:', content[1]

    def processElectionResult(self):
    # format content election result
    # JSON [senderAddress, 'WIN'] atau [senderAddress, 'LOSE']
        global isAlreadyVoted
        global isCandidate
        global leaderAddress
        global roundElection
        if (not isAlive):
            return # lagi di-pause
        content = json.loads(self.getContent())
        print 'Election result received:', content
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
        if ((not isAlive) or roundElection > 0): # lagi di-pause atau lagi di masa pemilihan leader
            return
        idxWorkerAddress = listWorkerLoadLeader.index(min(listWorkerLoadLeader)) # cari worker dengan load terkecil
        workerAddress = listWorkerAddress[idxWorkerAddress] + '/' + n
        print 'Requesting prime number to:', workerAddress
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
    def __init__(self, port = ''):
        print 'Starting... '
        self.handler = HTTPServer(("", port), NodeHandler)
        thread.start_new_thread(self.handler.serve_forever, ()) # nyalakan handler
        thread.start_new_thread(countdown, ()) # nyalakan countdown timeout

    def run(self):
    # node berganti peran antara leader, candidate, atau follower
        while(True):
            if (leaderAddress == address):
                print 'I am a leader!'
                self.leaderMain()
            elif (isCandidate):
                print 'I am a candidate!'
                self.candidateMain()
            else:
                print 'I am a follower.'
                self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        while (leaderAddress == address):
            if (isAlive):
                self.sendHeartbeat()

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        global isCandidate
        global leaderAddress
        global roundElection
        if (self.sendVoteRequest()): # minta vote, apakah mayoritas memilih dia
            isCandidate = False
            leaderAddress = address # terpilih jadi leader
            data = json.dumps(address, 'WIN')
            print 'Broadcasting election result: WIN'
            self.broadcastToOtherNodes('/election', data)
            roundElection = 0
        else:
            data = json.dumps(address, 'LOSE')
            print 'Broadcasting election result: LOSE'
            self.broadcastToOtherNodes('/election', data)
            roundElection += 1
            restoreCountdown()
            while (timeoutCountdown > 0):
                pass # 'sleep' dengan cara bukan sleep

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        global isCandidate
        global roundElection
        while ((roundElection == 0) and isAlive):
            if (timeoutCountdown <= 0):
                isCandidate = True
                roundElection += 1

    def broadcastToOtherNodes(self, command, data = ''):
        listDestinationAddress = [] # node selain node sendiri
        for nodeAddress in listNodeAddress:
            if (nodeAddress != address):
                listDestinationAddress.append(nodeAddress + command)
        job = (grequests.post(destinationAddress, data = data) for destinationAddress in listDestinationAddress)
        return grequests.map(job) # kirim

    def sendHeartbeat(self):
        data = [address, listWorkerLoad]
        data = json.dumps(data)
        print 'Broadcasting heartbeat:', listWorkerLoad
        listResponse = self.broadcastToOtherNodes('/heartbeat', data)
        print 'Processing heartbeat response:', listResponse
        # format content heartbeat response:
        # JSON [senderAddress, [listWorkerLoadSender]]
        # UNDER CONSTRUCTION

    def sendVoteRequest(self):
        print 'Broadcasting vote round:', roundElection
        listResponse = self.broadcastToOtherNodes('/vote', address)
        # format content vote response:
        # 'OK' or 'NO'
        print 'Processing vote response:', listResponse
        voteCount = 1 # dia pasti milih dia sendiri wk
        for response in listResponse:
            if (response == None):
                pass
            elif (response.content == 'OK'):
                voteCount += 1
        return (voteCount >= (len(listNodeAddress) / 2 + 1)) # hasil apakah terpilih mayoritas

################################################################################

def restoreCountdown(seconds = timeout):
    global timeoutCountdown
    timeoutCountdown = int(seconds * 1000)

def countdown():
    global timeoutCountdown
    timeBefore = int(time.time() * 1000)
    while(True):
        currentTime = int(time.time() * 1000)
        timeoutCountdown -= (currentTime - timeBefore)
        timeBefore = currentTime

################################################################################

address = ''
isAlive = True
isAlreadyVoted = False
isCandidate = False
leaderAddress = ''
listNodeAddress =[line.rstrip('\n') for line in open('listNodeAddress.txt')]
listWorkerAddress = [line.rstrip('\n') for line in open('listWorkerAddress.txt')]
listWorkerLoad = [maxload] * len(listWorkerAddress)
listWorkerLoadLeader = []
maxload = 1000 # jika worker mati maka elemen ybs pada listWorkerLoad = maxload
roundElection = 0
timeout = int(1)
timeoutCountdown = int(1000)

################################################################################

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
        timeout = int(sys.argv[2])
        restoreCountdown()
        Node(address.split(':')[2]).run()
    else:
        print 'address(es) not listed in listNodeAddress.txt'
else:
    print 'usage: node.py <IP:port> <timeout>'
