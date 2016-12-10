#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text received from the
 network every second.

 Usage: stateful_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming
    would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
/Users/pbarna/Desktop/apache-source/spark-2.0.2-bin-hadoop2.7/bin/spark-submit stateful_network_wordcount.py localhost 9999

    lines:
dataset;subscriber;TAC;type;timestamp;unix;latitude;longitude
10.03;580C1941;35512407;1;2016-10-03T08:13:25;1475475205;47.603111;19.060024
10.03;580C1941;35512407;1;2016-10-03T06:16:05;1475468165;47.748206;18.504456
10.03;580C1941;35512407;1;2016-10-03T14:02:40;1475496160;47.537370;19.138583
10.03;580C1941;35512407;1;2016-10-03T19:48:10;1475516890;47.537370;19.138583
10.03;580C1941;35512407;1;2016-10-03T19:37:10;1475516230;47.538025;19.149754
10.03;580C1941;35512407;1;2016-10-03T14:01:02;1475496062;47.538025;19.149754
10.03;580C1941;35512407;3;2016-10-03T06:26:54;1475468814;47.748206;18.504456
10.03;580C1941;35512407;3;2016-10-03T05:51:23;1475466683;47.748206;18.504456
10.03;580C1941;35512407;1;2016-10-03T05:44:06;1475466246;47.748206;18.504456
"""
from __future__ import print_function

import sys

import json
import httplib
from datetime import datetime, timedelta



def sendToDashBoard(obj):
    conn = httplib.HTTPConnection("asdasd.hu", 19200)
    json_data = json.dumps(convertFrame(obj))
    print(json_data)
    conn.request("POST", "/supa-jani/sample", json_data)
    resp = conn.getresponse()
    print(resp.read())


def sendallToDashBoard(rdd):
    for obj in rdd.take(rdd.count()):
        sendToDashBoard(obj)


#outputPath = '~/tmp/streamingdemo/output'
delimiter = ';'

def parseLine(line):
    """
    :param line:
    '10.03;580C1941;35512407;1;2016-10-03T05:44:06;1475466246;47.748206;18.504456'
    :return:

    """
    fields = line.split(delimiter)
    if not fields or len(fields) < 7:
        return None, None
    else:
        return {"subscriber": fields[0] + ';' + fields[1],
                "location": fields[6] + ';' + fields[7]
                }

def convertFrame(frm):
    """
    (u'47.537370;19.138583', 16)
    """
    lat, lon = frm[0].split(';')
    res = {'count': frm[1], 'location': {'lat': lat, 'lon': lon},
           'timestamp': (datetime.now() + timedelta(hours=-1)).isoformat()}
    print(res)
    return res




def main():
    from pyspark import SparkContext
    from pyspark.streaming import StreamingContext

    if len(sys.argv) != 3:
        print("Usage: stateful_network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingStatefulNetworkWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint")

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([])

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    # lambda line: getSubscriberAndLocation(line))\
    running_counts = lines.map(parseLine) \
                          .map(lambda m: (m['location'], 1))\
                          .updateStateByKey(updateFunc, initialRDD=initialStateRDD)

    #running_counts.pprint()
    running_counts.foreachRDD(sendallToDashBoard)


    ssc.start()
    ssc.awaitTermination()



if __name__ == "__main__":
    #o = parseLine('10.03;580C1941;35512407;1;2016-10-03T05:44:06;1475466246;47.748206;18.504456')
    #print(o)
    #print(sendToDashBoard(o))
    main()
