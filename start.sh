#!/bin/bash
cd /apps/DataLoad/output/
JAVA_PATH=`which java`
JAR_NAME=DataLoad.jar 
$JAVA_PATH -jar $JAR_NAME 192.168.1.231 root root 127.0.0.1 postgres postgres em_1147514 /tmp root bjiamcall
