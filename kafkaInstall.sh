#!/usr/bin/env bash

# download Kafka compressed file
curl http://apache.mirror.amaze.com.au/kafka/2.6.0/kafka_2.13-2.6.0.tgz -o kafka.tgz

#extract file
tar -xzf kafka.tgz

