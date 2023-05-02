#!/bin/bash

CP="$HOME/cassandra/build/classes/main:$HOME/cassandra/build/classes/thrift:$HOME/cassandra/lib/*:$HOME/cassandra/build/lib/jars/*:$HOME/cassandra/build/test/classes:$HOME/cassandra/test/conf"
java -classpath $CP org.openjdk.jmh.Main .*microbench.*BatchStatementBench -prof gc