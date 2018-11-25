#!/bin/bash -e

if [[ -z "$HOST_IP" ]]; then
    echo "Please set HOST_IP as docker machine ip or host ip"
    exit 1
fi

echo "Building mockstagram API"
cd mockstagram-api-master
npm install
docker build -t mockstagram-api .

cd ..
docker-compose up -d zookeeper kafka mockstagram-api

echo "Building java app"
cd mockstagram-streamer-java
mvn clean package
mvn docker:build

cd ..
echo "Starting 2 instance of app"
docker-compose up -d --scale mockstagram-streamer=2


echo "Waiting for a minute to get everything running"
sleep 60

echo "Seeding with 1000000 influencers"
docker run wurstmeister/kafka bash -c "seq 1000000 1099999 | /opt/kafka/bin/kafka-console-producer.sh --broker-list '$HOST_IP:9092' --topic 'pk'" > /dev/null

echo "Read the logs by running"
echo "$> docker-compose logs -f mockstagram-streamer"
