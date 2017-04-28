'''Kelas Daemon
Berfungsi mengirimkan hearthbeat berupa ID dan CPU usage.
Informasi dikirim setiap interval waktu.
Caranya, lakukan pemanggilan ke IP address:port'''

from __future__ import print_function
import os
import psutil
import socket
import requests
import grequests
import time
from flask import request
from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
interval = 2

class Daemon():
    def getCPUusage(self):
        return psutil.cpu_percent(interval=1)

    def getID(self):
        #ID using IP
        return socket.gethostbyname(socket.gethostname())

    def sendHeartBeat(self):
        myDaemon = {"id": self.getID(), "CPU usage": self.getCPUusage()}
        return myDaemon
    
    def broadcastToAllNodes(self):
        # kirim broadcast ke semua node
        job = (grequests.post(destNode+"/"+command, data = self.sendHeartBeat()) for destNode in listNodeAddress)
        print(job)
        return grequests.map(job)


listNodeAddress = [line.rstrip('\n') for line in open('listNodeAddress.txt')]
command = 'server'
if __name__ == "__main__":
    while True:
        daemon = Daemon()
        daemon.broadcastToAllNodes()