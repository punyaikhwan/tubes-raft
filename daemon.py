'''Kelas Daemon
Berfungsi mengirimkan hearthbeat berupa ID dan CPU usage.
Informasi dikirim setiap interval waktu.
Caranya, lakukan pemanggilan ke IP address:port'''

from __future__ import print_function
import os
import psutil
import socket
import grequests
import time
import json

interval = 2

class Daemon():
    def getCPUusage(self):
        return psutil.cpu_percent(interval=1)

    def getID(self):
        #ID using IP
        return socket.gethostbyname(socket.gethostname())

    def sendHeartBeat(self):
        myDaemon = {"id": self.getID(), "usage": self.getCPUusage()}
        return json.dumps(myDaemon)
    
    def broadcastToAllNodes(self):
        # kirim broadcast ke semua node
        job = (grequests.post(destNode+"/"+command, data = self.sendHeartBeat()) for destNode in listNodeAddress)
        content = grequests.map(job)
        print(content)
        return content


listNodeAddress = [line.rstrip('\n') for line in open('listNodeAddress.txt')]
command = 'server'
if __name__ == "__main__":
    while True:
        daemon = Daemon()
        daemon.broadcastToAllNodes()
        time.sleep(2)