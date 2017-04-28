import urllib2
import time
import json
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler

listServer = [line.rstrip('\n') for line in open('listServer.txt')]
PORT = 12345

class NodeHandler(BaseHTTPRequestHandler):
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
                elif (command == 'server'): #dari server
                    self.processHeartBeatServer()
                else:
                    self.sendResponse(400) # Bad Request
            else:
                self.sendResponse(400) # Bad Request
        except Exception as ex:
            self.sendResponse(500) # Internal Server Error

    def do_GET(self):
        try:
            args = self.path.split('/')
            if len(args) != 2:
                raise Exception()
            n = int(args[1])
            self.send_response(200)
            self.end_headers()
            self.content = urllib2.urlopen('http://localhost:13336').read()
            self.wfile.write(str(self.content).encode('utf-8'))
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)

    def processHeartBeatServer(self):
    	content_len = int(self.headers.getheader('content-length', 0))
    	post_body = self.rfile.read(content_len)
        haha = json.loads(post_body)
        print haha

server = HTTPServer(("", PORT), NodeHandler)
server.serve_forever()
