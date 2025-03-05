#!/bin/bash

cd ../target
RUNJAVA="$JAVA_HOME/bin/java"
JAR=web-client-1.0.0.jar
$RUNJAVA -jar $JAR
cd -
