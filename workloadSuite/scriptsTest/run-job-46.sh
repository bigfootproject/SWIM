../bin/hadoop jar ../WorkGen.jar org.apache.hadoop.examples.WorkGen -conf ../conf/workGenKeyValue_conf.xsl -r 1 workGenInput/part-00001 workGenOut-job46 1.5258789E-5 1.0 >> ../output/job-46.txt 2>> ../output/job-46.txt 
../bin/hadoop dfs -rmr workGenOut-job46
# inputSize 67108864