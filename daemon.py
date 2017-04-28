'''Kelas Daemon
Berfungsi mengirimkan hearthbeat berupa ID dan CPU usage.
Informasi dikirim setiap interval waktu.
Caranya, lakukan pemanggilan ke IP address:port'''

from __future__ import print_function
import os
import psutil
import socket
import requests
import time
from flask import request
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
            self.send_response(200)
            self.end_headers()
            self.wfile.write(str(self.sendHeartBeat()).encode('utf-8'))
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)
    
    def broadcastToAllNodes(self):
        # kirim broadcast ke semua node
        job = (grequests.post(destNode+"/"+command, data = self.sendHeartBeat()) for destNode in listNodeAddress)
        print(job)
        return grequests.map(job)

    def get_ip(self):
        """ Extract the client IP address from the HTTP request in a proxy-compatible way.

        @return: IP address as a string or None if not available
        """
        if "HTTP_X_FORWARDED_FOR" in request.environ:
            # Virtual host
            ip = request.environ["HTTP_X_FORWARDED_FOR"]
        elif "HTTP_HOST" in request.environ:
            # Non-virtualhost
            ip = request.environ["REMOTE_ADDR"]
        else:
            # Unit test code?
            ip = None

        return ip

listNodeAddress = [line.rstrip('\n') for line in open('listNodeAddress.txt')]
server = HTTPServer(("", PORT), DaemonHandler)
server.serve_forever()