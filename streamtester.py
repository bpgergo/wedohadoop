import socket
import sys
import logging
import time

class Class():

    def __init__(self, hostname='localhost', port=2101):
        self.version_3 = sys.version.startswith('3')
        server = socket.socket()  # self.server if it has to be '.close()'d
        self.server = server
        server.bind((socket.gethostbyname(hostname), port))
        server.listen(5)
        logging.debug("attempting to connect to ingester at host:{0}, port:{1}".format(hostname, port))
        self.client, _address = server.accept()

    def send(self, message):
        if self.version_3 and type(message) != type(b''):
            message = bytes(message, 'UTF-8')  # == message.encode('UTF-8')
        self.client.send(message)

    def clean_up(self):
        self.server.close()


def main():
    hostname='localhost'
    port=9999
    if len(sys.argv) > 1:
        hostname=sys.argv[1]
    if len(sys.argv) > 2:
        port=int(sys.argv[2])

    mess = Class(hostname, port)
    while True:
        mess.send("this dummy stream data and this\n")
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()