#!/bin/bash

export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64' 
export JAVA8_HOME='/usr/lib/jvm/java-8-openjdk-amd64' 
export JAVA11_HOME='/usr/lib/jvm/java-11-openjdk-amd64' 
export JDK_HOME='/usr/lib/jvm/java-8-openjdk-amd64'


cd $HOME/cassandra
ant clean > /dev/null
result_file=raw_result_after

if [ -d $result_file ]; then
    rm -f $result_file
fi

ant testsome \
-Dtest.name=org.apache.cassandra.repair.RepairJobCustomTest \
-Dtest.methods=releaseThreadAfterSessionForceShutdown \
&> raw_output.log

grep -F '!!!' raw_output.log > $result_file
rm raw_output.log
mv $result_file $HOME