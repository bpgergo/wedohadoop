import pandas as pd
import time
import socket
import random

crm_weekly = pd.read_csv("~/wedohadoop/data/msc_weekly.csv", delimiter=";")
SLEEP = 0.0001;


serversocket = socket.socket()
# bind the socket to a public host, and a well-known port
print("listening to connections")
serversocket.bind(("localhost", 9999))
serversocket.listen(5)
while True:
    # accept connections from outside
    (clientsocket, address) = serversocket.accept()
    print("Sending data in every ", SLEEP, "s")
    while True:
        time.sleep(SLEEP)
        record = random.randint(0,crm_weekly.shape[0]-1)
        print(";".join(str(x) for x in crm_weekly.ix[record]))
        clientsocket.send(bytes(";".join(str(x) for x in crm_weekly.ix[record])+"\n", 'UTF-8'))