#!/bin/bash

printf "\n=============================================\n"
printf "\nCopying input.txt File to /user/cloudera/input Directory\n"
hadoop fs -mkdir /user/cloudera/input/
hadoop fs -put /home/cloudera/Desktop/MRPairApproach/input/input.txt /user/cloudera/input/

printf "\n=============================================\n"
printf "\nDeleting Output Folder Contents\n"
hadoop fs -rm -r -f /user/cloudera/output

printf "\n=============================================\n"
printf "\nExecuting Project JAR File PAIR Approach\n"
hadoop jar pair-approach.jar com.an.mrproject.pair.MRPairDriver /user/cloudera/input/ /user/cloudera/output

printf "\n=============================================\n"
printf "\nReading INPUT File Contents\n"
hadoop fs -cat /user/cloudera/input/input.txt

printf "\n=============================================\n"
printf "\nFirst Reducer Output\n"
hadoop fs -cat /user/cloudera/output/part-r-00000

printf "\n=============================================\n"
printf "\nSecond Reducer Output\n"
hadoop fs -cat /user/cloudera/output/part-r-00001

printf "\n=============================================\n"
printf "\nExiting Now............\n"