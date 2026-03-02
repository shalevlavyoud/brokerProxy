@echo off
rem Run the Broker Proxy fat JAR (Windows)
rem Usage:  run.cmd
set JAR=target\broker-proxy-1.0.0-SNAPSHOT.jar
if not exist "%JAR%" (
    echo [run] JAR not found -- building first...
    call mvn package -DskipTests -q
)
java -jar "%JAR%" %*
