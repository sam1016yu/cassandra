#!/bin/bash
cur_dir=`pwd`
cd $HOME/cassandra
echo "Building..."
ant build-jmh > /dev/null
echo "Running BatchStatementBench..."
CP="$HOME/cassandra/build/classes/main:$HOME/cassandra/build/classes/thrift:$HOME/cassandra/lib/*:$HOME/cassandra/build/lib/jars/*:$HOME/cassandra/build/test/classes:$HOME/cassandra/test/conf"
java -classpath $CP org.openjdk.jmh.Main .*microbench.*BatchStatementBench -prof gc  &>  bench_output.log
grep "^BatchStatementBench.bench" bench_output.log > $HOME/result_after.out
echo "Done, cleaning now"
rm bench_output.log
ant clean > /dev/null
cd $cur_dir
