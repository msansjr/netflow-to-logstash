# netflow-to-logstash
Small standalone service that receives netflow packets from palo alto firewalls and forwards them to logstash via TCP/json

## Why?
The way that palo alto firewalls format netflow packets makes the logstash collector drop packets and waste too much CPU (https://github.com/logstash-plugins/logstash-codec-netflow/issues/85#issuecomment-434915163)

This program was designed from the ground up to allocate every UDP and Netflow buffer at the start so that its memory footprint could be small and fixed in time.

## How to compile
- Download the file, put it inside a structure as: netflow/main/netflow.go
- In this dir run `go get ./..`
- Run `go build netflow.go`

The executable will be generated

## Using it
The parameters are quite sane, I have been using them in production for months without problems, the important ones to customize are:
- netflowAddres: the IP:port that the exporter will listen on
- tcpOutputAddr: the IP:port that the exporter will connect via TCP to send the json data to (logstash)

__Do not run under windows! It will drop packets!__

The program does not fork at the beginning, so the systemd service must have `Type=simple` in its configuration.

## Can I use it for another firewall?
Yes, you can. There are some specific lines of code that are used to translate Palo Alto's interface numbering to a friendly format. Also, the code knows about the `userid` and `appid` that are custom fields defined for this product. I have added some TODOs to remove the interface conversion code, because it can be done in logstash.
