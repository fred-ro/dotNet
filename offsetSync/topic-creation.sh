#!/usr/bin/env bash

kafka-topics --bootstrap-server kafka:9092 --create --topic test --replication-factor 1 --partitions 3
kafka-console-producer --bootstrap-server kafka:9092 --topic test <<EOF
line 1
line 2
line 3
EOF

kafka-console-consumer --bootstrap-server kafka:9092 --topic test --group oldGroupId