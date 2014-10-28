#!/bin/bash
cd /apps/DataLoad/output/
JAVA_PATH=`which java`
JAR_NAME=DataLoad.jar 
$JAVA_PATH -jar $JAR_NAME 192.168.1.229 root root 192.168.1.231 postgres postgres em_1147061 /tmp/ root bjiamcall
