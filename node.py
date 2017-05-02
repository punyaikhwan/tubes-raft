import grequests
import json
import sys
from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from thread import start_new_thread
from time import time
from urllib2 import urlopen

################################################################################

class NodeHandler(BaseHTTPRequestHandler):
    def getContent(self): # mengambil data yang terkandung dalam content HTTP request
        contentLength = int(self.headers.getheader('content-length', 0))
        return self.rfile.read(contentLength)

    def sendResponse(self, code = 200, data = ''): # mengirim response dari HTTP request, dengan code dan data yang custom
        self.send_response(code)
        self.end_headers()
        self.wfile.write(str(data).encode('utf-8'))

    def sendHeartbeatResponse(self):
    # format content heartbeat:
    # JSON [senderAddress, [listWorkerLoadSender]]
        global isRestored
        global leaderAddress
        global listWorkerLoadLeader
        if ((not isAlive) or roundElection > 0): # lagi di-pause atau lagi di masa pemilihan leader
            return
        content = json.loads(self.getContent())
        print 'Heartbeat received from', content[0], ':', content[1]
        if (content[0] != leaderAddress): # heartbeat bukan dari leader sekarang
            # kasus ketika dia leader, trus pause, trus resume: merasa dirinya masih menjadi leader
            # atau kasus ketika dia follower, trus pause, trus resume: ngikut aja orang gak tau apa2
            if (isRestored):
                leaderAddress = content[0] # otomatis jadi follower
                isRestored = False
        restoreCountdown() # leader masih hidup, timeout dikembalikan ke awal
        listWorkerLoadLeader = content[1] # copy data dari leader
        data = json.dumps([address, listWorkerLoad])
        print 'Sending heartbeat response:', listWorkerLoad
        self.sendResponse(200, data)

    def sendVoteResponse(self):
    # format content vote:
    # senderAddress
        global isAlreadyVoted
        global isCandidate
        global isIncomingVote
        global isRestored
        global leaderAddress
        global roundElection
        if (not isAlive): # lagi di-pause
            return
        else:
            isIncomingVote = True # untuk break sleepByTimeout()
        print 'Vote request received from ', self.getContent()
        if (isRestored): # sama kasusnya, pas mati trus hidup lagi, ngikut aja
            leaderAddress = ''
            isRestored = False
        elif (isCandidate): # kasus ketika dia juga candidate, tapi kalah cepet buat ngirim
            isCandidate = False
        if (isAlreadyVoted): # gabisa vote lebih dari sekali
            print 'Sending vote response: NO'
            self.sendResponse(200, 'NO')
        else:
            isAlreadyVoted = True
            roundElection += 1
            print 'Sending vote response: OK'
            self.sendResponse(200, 'OK')
        isIncomingVote = False

    def processVoteResult(self):
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
        isAlreadyVoted = False
        if (content[1] == 'WIN'): # candidate menang, ketua baru
            leaderAddress = content[0]
            roundElection = 0
        else:
            isCandidate = True # candidate kalah, dia jadi candidate round selanjutnya
            roundElection += 1
            sleepByTimeout()
        self.sendResponse()

    def pause(self):
        global isAlive
        if (isCandidate):
            self.sendResponse(200, 'Can not shut down at this time.')
        elif (isAlive):
            isAlive = False
            print 'Node now down.'
            self.sendResponse(200, 'Node now down.')
        else:
            self.sendResponse(200, 'Node already down.')

    def resume(self):
        global isAlive
        global isAlreadyVoted
        global isRestored
        global roundElection
        if (isAlive):
            self.sendResponse(200, 'Node already up.')
        else:
            # kembalikan nilai default
            restoreCountdown(60) # timeout di lama-lamain supaya nerima heartbeat
            isAlreadyVoted = False
            roundElection = 0
            # hidupkan
            isAlive = True
            isRestored = True
            print 'Node now up.'
            self.sendResponse(200, 'Node now up.')

    def processHeartbeatServer(self):
    # format content heartbeat server:
    # JSON [senderAddress, senderLoad]
        global listWorkerLoad
        if (not isAlive):
            return # lagi di-pause
        content = json.loads(self.getContent())
        idxWorkerAddress = listWorkerAddress.index(content['id'])
        listWorkerLoad[idxWorkerAddress] = content['usage']
        print 'Workload info', content['id'], 'updated:', content['usage']
        self.sendResponse()

    def sendPrimeRequest(self, n):
        if ((not isAlive) or roundElection > 0): # lagi di-pause atau lagi di masa pemilihan leader
            return
        idxWorkerAddress = listWorkerLoadLeader.index(min(listWorkerLoadLeader)) # cari worker dengan load terkecil
        workerAddress = listWorkerAddress[idxWorkerAddress] + '/' + n
        print 'Requesting prime number to:', workerAddress
        prime = str(urlopen(workerAddress).read()) # kirim request
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
                    self.processVoteResult()
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error
            print ex

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
        self.handler = HTTPServer(("", int(port)), NodeHandler)
        start_new_thread(self.handler.serve_forever, ()) # nyalakan handler
        start_new_thread(countdown, ()) # nyalakan countdown timeout

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
            sleepByTimeout()

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        global isCandidate
        global roundElection
        while ((roundElection == 0) and isAlive):
            if (timeoutCountdown < 0):
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
        global listWorkerLoad
        global listWorkerLoadLeader
        print 'Broadcasting heartbeat:', listWorkerLoad
        data = json.dumps([address, listWorkerLoad])
        listResponse = self.broadcastToOtherNodes('/heartbeat', data)
        print 'Processing heartbeat response:', listResponse
        # format content heartbeat response:
        # JSON [followerAddress, [listWorkerLoadFollower]]
        listWorkerLoadFollower = [0] * len(listWorkerAddress)
        for response in listResponse:
            if (response is None):
                pass
            else:
                content = json.loads(response.content)
                for i in range(len(content[1])):
                    listWorkerLoadFollower[i] += int(content[1][i])
        # update berdasarkan mayoritas
        for i in range(len(listWorkerAddress)):
            workerLoadFollower = listWorkerLoadFollower[i]
            # kalo mayoritas bilang mati, matiin
            if (workerLoadFollower >= (maxload * (len(listNodeAddress) / 2 + 1))):
                listWorkerLoad[i] = maxload
                listWorkerLoadLeader[i] = maxload
            # kalo nggak, itung rata-ratanya
            else:
                average = (workerLoadFollower % maxload) / len(listNodeAddress)
                listWorkerLoad[i] = average
                listWorkerLoadLeader[i] = average

    def sendVoteRequest(self):
        print 'Broadcasting vote, round:', roundElection
        listResponse = self.broadcastToOtherNodes('/vote', address)
        # format content vote response:
        # 'OK' or 'NO'
        print 'Processing vote response:', listResponse
        voteCount = 1 # dia pasti milih dia sendiri wk
        for response in listResponse:
            if (response is None):
                pass
            elif (response.content == 'OK'):
                voteCount += 1
        return (voteCount >= (len(listNodeAddress) / 2 + 1)) # hasil apakah lebih dari kuorum

################################################################################

def sleepByTimeout(): # 'sleep' dengan cara bukan sleep
    restoreCountdown()
    while ((timeoutCountdown > 0) and (not isIncomingVote)):
        pass

def restoreCountdown(seconds = None):
    global timeoutCountdown
    if (seconds is None):
        seconds = timeout
    timeoutCountdown = int(seconds * 1000)

def countdown():
    global timeoutCountdown
    timeBefore = int(time() * 1000)
    while(True):
        currentTime = int(time() * 1000)
        timeoutCountdown -= (currentTime - timeBefore)
        timeBefore = currentTime

################################################################################

maxload = 1000 # jika worker mati maka elemen ybs pada listWorkerLoad = maxload

address = ''
isAlive = True
isAlreadyVoted = False
isCandidate = False
isIncomingVote = False
isRestored = False
leaderAddress = ''
listNodeAddress =[line.rstrip('\n') for line in open('listNodeAddress.txt')]
listWorkerAddress = [line.rstrip('\n') for line in open('listWorkerAddress.txt')]
listWorkerLoad = [maxload] * len(listWorkerAddress)
listWorkerLoadLeader = [maxload] * len(listWorkerAddress)
roundElection = 0
timeout = int()
timeoutCountdown = int()

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
    port = address.split(':')[2]
    # cek apakah IP:port ada di list
    if (address in listNodeAddress):
        timeout = int(sys.argv[2])
        restoreCountdown()
        Node(port).run()
    else:
        print 'address(es) not listed in listNodeAddress.txt'
else:
    print 'usage: node.py <IP:port> <timeout>'
