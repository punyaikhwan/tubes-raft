'''Kelas Daemon
Berfungsi mengirimkan hearthbeat berupa ID dan CPU usage.
Informasi dikirim setiap interval waktu.
Caranya, lakukan pemanggilan ke IP address:port'''

from __future__ import print_function
import os
import psutil
import socket
import time
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
PORT = 13336
interval = 2

class DaemonHandler(BaseHTTPRequestHandler):

    def getCPUusage(self):
        return psutil.cpu_percent(interval=1)

    def getID(self):
        #ID using IP
        return socket.gethostbyname(socket.gethostname())

    def sendHeartBeat(self):
        myDaemon = {"id": self.getID(), "CPU usage": self.getCPUusage()}
        return myDaemon

    def do_GET(self):
        try:
            while True:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(str(self.sendHeartBeat()).encode('utf-8'))
                time.sleep(interval)
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)

server = HTTPServer(("", PORT), DaemonHandler)
server.serve_forever()
