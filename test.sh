#!/bin/bash

# create local dirs for data and get the data

mkdir -p projekt_local/data
cd projekt_local/data
wget https://datasets.imdbws.com/title.basics.tsv.gz
gunzip title.basics.tsv.gz
wget https://datasets.imdbws.com/title.principals.tsv.gz
gunzip title.principals.tsv.gz
cd ..
cd ..



# create hadoop fs dirs for the data and copyFromLocal
cd projekt_local/data
hadoop fs -mkdir -p projekt1/hadoop/mapreduce/input
hadoop fs -copyFromLocal title.principals.tsv projekt1/hadoop/mapreduce/input/title.principals.tsv
cd ..
cd ..


# mapreduce
hadoop jar wordcount.jar WordCount projekt1/hadoop/mapreduce/input projekt1/hadoop/mapreduce/output



# copyFromLocal basics
cd projekt_local/data
hadoop fs -mkdir projekt1/basics
hadoop fs -copyFromLocal title.basics.tsv projekt1/basics
cd ..
cd ..



# hive sql
hive -f file1.sql



# result
cd projekt_local
mkdir result
cd result
hadoop fs -copyToLocal /user/$USER/result/000000_0
cat 000000_0
cd ..
cd ..

