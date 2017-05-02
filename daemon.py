'''Kelas Daemon
Berfungsi mengirimkan hearthbeat berupa ID dan CPU usage.
Informasi dikirim setiap delay waktu.
Caranya, lakukan request ke IP address:port'''

# MODULE
import grequests
import json
import psutil
import sys
import time

# PROSEDUR
def getUsage():
    return psutil.cpu_percent(interval = 1)

def getData():
    return json.dumps({"id": address, "usage": getUsage()})

def broadcastToAllNodes():
    data = getData()
    print 'Broadcasting workload info:', data
    # kirim broadcast ke semua node
    job = (grequests.post(nodeAddress + "/server", data = data) for nodeAddress in listNodeAddress)
    responses = grequests.map(job)
    print 'Responses:', responses

# VARIABLE
address = ''
listNodeAddress = [line.rstrip('\n') for line in open('listNodeAddress.txt')]
listWorkerAddress = [line.rstrip('\n') for line in open('listWorkerAddress.txt')]
delay = 2

# PROGRAM UTAMA
if (len(sys.argv) == 2):
    address = 'http://' + sys.argv[1]
    # cek apakah IP:port ada di list
    if (address not in listWorkerAddress):
        print address, 'not listed in listWorkerAddress.txt'
    else:
        # run daemon
        while True:
            broadcastToAllNodes()
            time.sleep(delay)
else:
    print 'usage: daemon.py <IP:port>'
