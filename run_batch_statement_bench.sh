#!/bin/bash
cur_dir=`pwd`
cd $HOME/cassandra
ant build-jmh
CP="$HOME/cassandra/build/classes/main:$HOME/cassandra/build/classes/thrift:$HOME/cassandra/lib/*:$HOME/cassandra/build/lib/jars/*:$HOME/cassandra/build/test/classes:$HOME/cassandra/test/conf"
java -classpath $CP org.openjdk.jmh.Main .*microbench.*BatchStatementBench -prof gc 2>&1 | tee bench_output.log
grep "^BatchStatementBench.bench" bench_output.log > $HOME/result_before.out
rm bench_output.log
ant clean
cd $cur_dir