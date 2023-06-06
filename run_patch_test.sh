#!/bin/bash

cd $HOME/cassandra
ant clean > /dev/null

result_file=raw_result_before

if [ -d $result_file ]; then
    rm -f $result_file
fi

ant testsome \
-Dtest.name=org.apache.cassandra.io.sstable.SSTableMetadataTest \
-Dtest.methods=trackClusteringValuesMinimize \
&> raw_output.log

grep -F '!!!' raw_output.log > $result_file

rm raw_output.log

mv $result_file $HOME
