#!/usr/bin/env bash

# start zookeeper server
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

#start kafka server (single broker)
nohup bin/kafka-server-start.sh config/server.properties &


