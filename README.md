# Mockstagram with Kafka Streams

## Dependencies
 - docker
 - docker-compose
 - Maven
 - npm

## Instructions to run
 1. Set `$HOST_IP` with docker machine IP or host IP if using docker for mac.
 2. Run `./run.sh`

It will build all the docker containers and start the system with docker-compose. 

## To test
 1. Find out which port the `mockstagram-streamer` instance is bound to by running `docker-compose ps`
 2. Get influencer stats with one minute window: `curl http://localhost:<port>/api/influencers/1000000`
 3. Get latest influencer stat: `curl http://localhost:<port>/api/influencers/1000000/latest`
 4. Get influencer rank: `curl http://localhost:<port>/api/influencers/1000000/rank`
 5. Get average followers: `curl http://localhost:<port>/api/influencers/average`

## Note
 1. The kafka consumers are not configured to run on both instances. If one of them is not working, try to call the other instance for the API data.
