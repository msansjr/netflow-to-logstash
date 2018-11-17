# netflow-to-logstash
Small standalone service that receives netflow packets from palo alto firewalls and forwards them to logstash via TCP/json

# Why?
The way that palo alto firewalls format netflow packets makes the logstash collector drop packets and waste too much CPU (https://github.com/logstash-plugins/logstash-codec-netflow/issues/85#issuecomment-434915163)

# How to compile
- Download the file, put it inside a structure as: netflow/main/netflow.go
- In this dir run `go get ./..`
- Run `go build netflow.go`
The executable will be generated

# Using it
The parameters are quite sane, I have been using them in production for months and no problems, the important ones to customize are:
- netflowAddres: the IP:port that the exporter will listen on
- tcpOutputAddr: the IP:port that the exporter will connect via TCP to send the json data to (logstash)
