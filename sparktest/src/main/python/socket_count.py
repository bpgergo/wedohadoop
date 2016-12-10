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
 Counts words in text encoded with UTF8 received from the network every second.

 Usage: recoverable_network_wordcount.py <hostname> <port> <checkpoint-directory> <output-file>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
   data. <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
   <output-file> file to which the word counts will be appended

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`

 and then run the example
/Users/pbarna/Desktop/apache-source/spark-2.0.2-bin-hadoop2.7/bin/spark-submit sparktest/src/main/python/socket_count.py localhost 9999 ~/tmp/streamingdemo/ ~/tmp/output

 If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
 checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
 the checkpoint data.
"""
from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


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
        return {#"subscriber": fields[0] + ';' + fields[1],
                "location": fields[6] + ';' + fields[7]
                }

def createContext(host, port, outputPath):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    if os.path.exists(outputPath):
        os.remove(outputPath)
        print('filedeleteeeeee =============================================================================')
    sc = SparkContext(appName="PythonStreamingRecoverableNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([])

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines = ssc.socketTextStream(host, port)
    running_counts = lines.map(parseLine) \
                          .map(lambda m: (m['location'], 1))\
                          .updateStateByKey(updateFunc, initialRDD=initialStateRDD)


    def echo(time, rdd):
        # Get or register the blacklist Broadcast
        #blacklist = getWordBlacklist(rdd.context)
        # Get or register the droppedWordsCounter Accumulator
        #droppedWordsCounter = getDroppedWordsCounter(rdd.context)

        # Use blacklist to drop words and use droppedWordsCounter to count them
        def filterFunc(wordCount):
            #if wordCount[0] in blacklist.value:
            #    droppedWordsCounter.add(wordCount[1])
            #    False
            #else:
            return True

        counts = "Counts at time %s %s" % (time, rdd.filter(filterFunc).collect())
        print(counts)
        #print("Dropped %d word(s) totally" % droppedWordsCounter.value)
        #print("Appending to " + os.path.abspath(outputPath))
        with open(outputPath, 'a') as f:
            f.write(counts + "\n")

    running_counts.foreachRDD(echo)
    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: recoverable_network_wordcount.py <hostname> <port> "
              "<checkpoint-directory> <output-file>", file=sys.stderr)
        exit(-1)
    host, port, checkpoint, output = sys.argv[1:]
    ssc = StreamingContext.getOrCreate(checkpoint,
        lambda: createContext(host, int(port), output))
    ssc.start()
    ssc.awaitTermination()
