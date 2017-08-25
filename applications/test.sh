#!/bin/bash

tmpfile=${1-small.graphx}
outfile="$tmpfile"_out
hadoop fs -rm $tmpfile
hadoop fs -rm -r $outfile
hadoop fs -copyFromLocal data/small.graphx $tmpfile
spark-submit --driver-memory 80G --executor-memory 80G --executor-cores 88 \
    --driver-cores 88 --class "com.intel.ClosingTheGap.Pagerank" \
    target/scala-2.10/closing_the_gap_2.10-1.0.jar $tmpfile 4 0 54 native \
    $outfile
hadoop fs -getmerge /btg/small.graphx_out merged
hadoop fs -rm $tmpfile
hadoop fs -rm -r $outfile
diff merged results/pagerank_ref.txt
result=$?
if [ $result -eq 0 ]
then 
    echo "PASSED"
else
    echo "FAILED"
fi
rm merged
exit $result
