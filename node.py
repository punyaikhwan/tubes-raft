import json
import requests
import grequests
import urllib2
import sys
import thread
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

# KONSTANTA
maxload = 1000 # jika worker mati maka elemen ybs pada listWorkerLoad = maxload

# GLOBAL VARIABLE, DIPAKE DI Node DAN NodeHandler
isAlreadyVoted = False
isAlive = True
listWorkerLoad = []
listWorkerLoadLeader = []


class NodeHandler(BaseHTTPRequestHandler):
    def sendResponse(self, code, data = ''):
        self.send_response(code)
        self.end_headers()
        self.wfile.write(str(data).encode('utf-8'))

    def sendHeartbeatResponse(self):
    # mengirim heartbeat response, dilakukan oleh follower/candidate lain
        global listWorkerLoadLeader
        # membaca pesan pada heartbeat
        content_len = int(self.headers.getheader('content-length', 0))
        post_body = self.rfile.read(content_len)
        # copy data leader
        listWorkerLoadLeader = json.loads(post_body)
        # response dengan punya dia
        self.sendResponse(200, "Copied")

    def sendVoteResponse(self):
    # mengirim vote response, dilakukan oleh follower
        global isAlreadyVoted
        if (isAlreadyVoted):
            self.sendResponse(200, 'NO')
        else:
            self.sendResponse(200, 'OK')
            isAlreadyVoted = True

    def processHeartbeatServer(self):
        global listWorkerLoad
        content_len = int(self.headers.getheader('content-length', 0))
        post_body = self.rfile.read(content_len)

    def pause(self):
    # simulasi ketika node mati
        global isAlive
        if (isAlive):
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
            isAlive = True
            self.sendResponse(200, 'Node now up.')

    def sendPrimeRequest(self, n):
    # meminta bilangan prima ke worker, dilakukan oleh leader/follower
        # cari worker dengan load terkecil
        idxWorkerAddress = listWorkerLoad.index(min(listWorkerLoad))
        workerAddress = self.listWorkerAddress[idxWorkerAddress] + '/'
        # request bilangan prima
        prime = urllib2.urlopen(workerAddress + n).read()
        # kirim hasilnya
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
                n = int(args[2])
                if (command == 'prime'):
                    self.sendPrimeRequest(n)
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error
            print ex


















class Node():
    def __init__(self, address, timeout, listWorkerAddress = []):
    # create objek
        print 'Creating... ' + address
        global listWorkerLoad
        self.address = address # ID node ini
        self.timeout = int(timeout)
        self.listWorkerAddress = listWorkerAddress
        listWorkerLoad = [0] * len(listWorkerAddress)

    def initialize(self, leaderAddress, listNodeAddress = []):
    # node pertama kali dinyalakan
        print 'Starting... ' + address
        global isAlive
        global isAlreadyVoted
        self.leaderAddress = leaderAddress
        self.listNodeAddress = listNodeAddress
        self.listIsNodeAlive = [True] * len(listNodeAddress)
        isAlive = True
        self.isCandidate = False
        isAlreadyVoted = False
        # nyalakan handler
        portNode = self.address.split(':')[2]
        self.handler = HTTPServer(("", int(portNode)), NodeHandler)
        thread.start_new_thread(self.handler.serve_forever, ())
        # mulai kerja
        self.nodeMain()

    def nodeMain(self):
    # program utama node, memilih peran sebagai apa
        while(True):
            print 'Determining job... ' + address
            if (self.leaderAddress == self.address):
                self.leaderMain()
            elif (self.isCandidate):
                self.candidateMain()
            else:
                self.followerMain()

    def leaderMain(self):
    # program utama ketika berperan sebagai leader
        print 'I am a leader! ' + address
        global isAlive
        while (True):
            if (isAlive):
                # UNDER CONSTRUCTION, konsep doang ini
                self.sendHeartbeat()

    def followerMain(self):
    # program utama ketika berperan sebagai follower
        print 'I am a follower. ' + address
        global isAlreadyVoted
        isAlreadyVoted = False
        while (True):
            if (isAlive):
                # UNDER CONSTRUCTION, konsep doang ini
                timeout = False
                if (timeout):
                    self.isCandidate = True
                    break

    def candidateMain(self):
    # program utama ketika berperan sebagai candidate leader
        print 'I am a candidate! ' + address
        if (self.sendVoteRequest()):
            self.leaderAddress = self.address
            self.isCandidate = False

    def broadcastToOtherNodes(self, command, data =''):
        # persiapkan node tujuan
        listDestinationAddress = []
        for nodeAddress in listNodeAddress:
            if (nodeAddress != self.address):
                listDestinationAddress.append(nodeAddress + command)
        # kirim
        job = (grequests.post(destinationAddress, data = data) for destinationAddress in listDestinationAddress)
        return grequests.map(job)

    def sendHeartbeat(self):
    # mengirim heartbeat, dilakukan oleh leader
        print 'Sending heartbeat... ' + address
        # persiapkan data dan kirim
        data = json.dumps(listWorkerLoad)
        listResponse = self.broadcastToOtherNodes('/heartbeat', data)

    def sendVoteRequest(self):
    # mengirim vote, dilakukan oleh candidate
        print 'Requesting vote... ' + address
        # kirim
        responses = self.broadcastToOtherNodes('/vote')
        # proses responses
        voteCount = 0
        for response in responses:
            if (response.content == 'OK'):
                voteCount += 1
        # hasil apakah terpilih mayoritas
        return (voteCount >= (len(list_node) / 2 + 1))

#baca daftar server dari file txt dan memasukkannya ke list
listWorkerAddress = [line.rstrip('\n') for line in open('listWorkerAddress.txt')]
listNodeAddress = [line.rstrip('\n') for line in open('listNodeAddress.txt')]

# PROGRAM UTAMA
address = 'http://localhost:13000'
Node(address, 1).initialize(address, [address])