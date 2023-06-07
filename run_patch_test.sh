#!/bin/bash

export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64' 
export JAVA8_HOME='/usr/lib/jvm/java-8-openjdk-amd64' 
export JAVA11_HOME='/usr/lib/jvm/java-11-openjdk-amd64' 
export JDK_HOME='/usr/lib/jvm/java-8-openjdk-amd64'

ant testsome \
-Dtest.name=org.apache.cassandra.repair.RepairJobCustomTest \
-Dtest.methods=releaseThreadAfterSessionForceShutdown \
2>&1 | tee raw_output.log
# &> raw_output.log