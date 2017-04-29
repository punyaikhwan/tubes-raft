import grequests
import json
import requests
import sys
import thread
import urllib2
from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer

################################################################################

class NodeHandler(BaseHTTPRequestHandler):
    def getContent(self):
        contentLength = int(self.headers.getheader('content-length', 0))
        return self.rfile.read(contentLength)

    def sendResponse(self, code, data = ''):
        self.send_response(code)
        self.end_headers()
        self.wfile.write(str(data).encode('utf-8'))

    def sendHeartbeatResponse(self):
    # mengirim heartbeat response, dilakukan oleh follower
        global isAlreadyVoted
        global leaderAddress
        global listWorkerLoadLeader
        # membaca pesan pada heartbeat
        print 'Reading heartbeat...'
        content = json.loads(self.getContent())
        # cek apakah ini heartbeat dari leader baru
        if (content[0] != leaderAddress):
            leaderAddress = content[0]
            isAlreadyVoted = False
        # copy data dari leader
        listWorkerLoadLeader = content[1]
        # response dengan punya dia
        print 'Sending heartbeat response...'
        data = json.dumps(listWorkerLoad)
        self.sendResponse(200, data)

    def sendVoteResponse(self):
    # mengirim vote response, dilakukan oleh follower
        global isAlreadyVoted
        print 'Vote request received from ', self.getContent()
        if (isAlreadyVoted):
            print 'Sending vote response: OK'
            self.sendResponse(200, 'NO')
        else:
            print 'Sending vote response: NO'
            self.sendResponse(200, 'OK')
            isAlreadyVoted = True

    def processHeartbeatServer(self):
        global listWorkerAddress
        global listWorkerLoad
        # membaca data worker
        content = json.loads(self.getContent())
        workerAddress = content[0]
        print 'Workload info received from ', workerAddress
        # simpan data workload
        idxWorkerAddress = self.listWorkerAddress.index(workerAddress)
        listWorkerLoad[idxWorkerAddress] = content[1]

    def pause(self):
    # simulasi ketika node mati
        global isAlive
        if (isAlive):
            print 'Paused.'
            isAlive = False
            self.sendResponse(200, 'Node now down.')
        else:
            self.sendResponse(200, 'Node already down.')

    def resume(self):
    # simulasi ketika node hidup kembali
        global isAlive
        if (isAlive):
            self.sendResponse(200, 'Node already up.')
        else:
            print 'Resuming...'
            isAlive = True
            self.sendResponse(200, 'Node now up.')

    def sendPrimeRequest(self, n):
    # meminta bilangan prima ke worker, dilakukan oleh leader/follower
        print 'Requesting prime number to worker...'
        # cari worker dengan load terkecil
        idxWorkerAddress = listWorkerLoad.index(min(listWorkerLoad))
        workerAddress = listWorkerAddress[idxWorkerAddress] + '/' + n
        # request bilangan prima
        prime = str(urllib2.urlopen(workerAddress).read())
        # kirim hasilnya
        print 'Sending prime number response: ', prime
        self.sendResponse(200, prime)

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
                elif (command == 'server'): # dari server
                    self.processHeartbeatServer()
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error

    def do_GET(self):
    # menghandle request HTTP GET yang masuk ke node ini (dari client)
        try:
            args = self.path.split('/')
            # mengecek command apa yang diminta client
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
                if (command == 'prime'):
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
    def initialize(self):
    # node pertama kali dinyalakan
        global address
        print 'Starting... ' + address
        self.isCandidate = False
        # nyalakan handler
        portNode = address.split(':')[2]
        self.handler = HTTPServer(("", int(portNode)), NodeHandler)
        thread.start_new_thread(self.handler.serve_forever, ())

    def run(self):
    # program utama node, memilih peran sebagai apa
        global address
        global leaderAddress
        while(True):
            print 'Determining job...'
            if (leaderAddress == address):
                self.leaderMain()
            elif (self.isCandidate):
                self.candidateMain()
            else:
                self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        global isAlive
        print 'I am a leader!'
        while (True):
            if (isAlive):
                # UNDER CONSTRUCTION, konsep doang ini
                self.sendHeartbeat()

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        global isAlive
        print 'I am a follower.'
        while (True):
            if (isAlive):
                # UNDER CONSTRUCTION, konsep doang ini
                isLeaderDead = False
                if (isLeaderDead):
                    self.isCandidate = True
                    break

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        global address
        global leaderAddress
        print 'I am a candidate!'
        # minta vote, apakah mayoritas memilih dia
        if (self.sendVoteRequest()):
            self.isCandidate = False
            leaderAddress = address # terpilih jadi leader

    def broadcastToOtherNodes(self, command, data = ''):
        global address
        global listNodeAddress
        # persiapkan node tujuan
        listDestinationAddress = []
        for nodeAddress in listNodeAddress:
            if (nodeAddress != address):
                listDestinationAddress.append(nodeAddress + command)
        # kirim
        job = (grequests.post(destinationAddress, data = data) for destinationAddress in listDestinationAddress)
        return grequests.map(job)

    def sendHeartbeat(self):
    # mengirim heartbeat, dilakukan oleh leader
        print 'Sending heartbeat...'
        # persiapkan data dan kirim
        data = [address, listWorkerLoad]
        data = json.dumps(data)
        listResponse = self.broadcastToOtherNodes('/heartbeat', data)
        # lakukan konsensus (dapatkan mayoritas)
        # UNDER CONSTRUCTION

    def sendVoteRequest(self):
    # mengirim vote, dilakukan oleh candidate
        global address
        print 'Requesting vote...'
        listResponse = self.broadcastToOtherNodes('/vote', address)
        # proses listResponse
        voteCount = 0
        for response in listResponse:
            if (response.content == 'OK'):
                voteCount += 1
        # hasil apakah terpilih mayoritas
        return (voteCount >= (len(list_node) / 2 + 1))

################################################################################

# KONSTANTA
maxload = 1000 # jika worker mati maka elemen ybs pada listWorkerLoad = maxload

# GLOBAL VARIABLE, DIPAKE DI Node DAN NodeHandler
address = 'http://localhost:13000'
isAlive = True
isAlreadyVoted = False
leaderAddress = 'http://localhost:13000'
listNodeAddress =[line.rstrip('\n') for line in open('listNodeAddress.txt')]
listWorkerAddress = [line.rstrip('\n') for line in open('listWorkerAddress.txt')]
listWorkerLoad = [maxload] * len(listWorkerAddress)
listWorkerLoadLeader = []
timeout = 1

# PROGRAM UTAMA
node = Node()
node.initialize()
node.run()
