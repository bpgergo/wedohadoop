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


def send_dummy(mess):
    mess.send("""10.03;580C1941;35512407;1;2016-10-03T08:13:25;1475475205;47.603111;19.060024
    10.03;580C1941;35512407;1;2016-10-03T06:16:05;1475468165;47.748206;18.504456
    10.03;580C1941;35512407;1;2016-10-03T14:02:40;1475496160;47.537370;19.138583
    10.03;580C1941;35512407;1;2016-10-03T19:48:10;1475516890;47.537370;19.138583
    10.03;580C1941;35512407;1;2016-10-03T19:37:10;1475516230;47.538025;19.149754
    10.03;580C1941;35512407;1;2016-10-03T14:01:02;1475496062;47.538025;19.149754
    10.03;580C1941;35512407;3;2016-10-03T06:26:54;1475468814;47.748206;18.504456
    10.03;580C1941;35512407;3;2016-10-03T05:51:23;1475466683;47.748206;18.504456
    10.03;580C1941;35512407;1;2016-10-03T05:44:06;1475466246;47.748206;18.504456
    """)


def main():
    hostname='localhost'
    port=9999
    if len(sys.argv) > 1:
        hostname=sys.argv[1]
    if len(sys.argv) > 2:
        port=int(sys.argv[2])

    mess = Class(hostname, port)
    while True:
        fname = '/Users/pbarna/Documents/gergo/bigdatahackathon/msc_weekly.csv'
        with open(fname) as f:
            mess.send(f.read())
        time.sleep(0.5)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()