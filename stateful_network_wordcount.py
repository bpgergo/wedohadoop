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


outputPath = '~/tmp/streamingdemo/output'
delimiter = ';'

def getSubscriberAndLocation(line):
    vals = line.split(delimiter)
    if not vals or len(vals) < 7:
        return None, None
    else:
        return vals[0]+'-'+vals[1], vals[6]+'-'+vals[7]

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
    running_counts = lines.map(getSubscriberAndLocation) \
                          .map(lambda tuple: (tuple[1], 1))\
                          .updateStateByKey(updateFunc, initialRDD=initialStateRDD)

    running_counts.pprint()
    """
    def echo(time, rdd):
        counts = "Counts at time %s %s" % (time, rdd.collect())
        print(counts)
        with open(outputPath, 'a') as f:
            f.write(counts + "\n")

    running_counts.foreachRDD(echo)
    """
    ssc.start()
    ssc.awaitTermination()



if __name__ == "__main__":
    #tuple = getSubscriberAndLocation('10.03;580C1941;35512407;1;2016-10-03T05:44:06;1475466246;47.748206;18.504456')
    #print(tuple[1])
    main()