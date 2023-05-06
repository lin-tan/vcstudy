#!/bin/bash
cd instrument-libs
mvn clean verify
cd ..
JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64 mvn clean verify exec:java -Dexec.mainClass="pfl.RPCLogServer" -Dexec.classpathScope="compile"