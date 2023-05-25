# twitter_streams_with_kafka

This project is an experiment learning Apache Kafka.
It takes streams of tweets on a particular topic (trending topic), parses the tweets and loads it into a mongo DB cluster

It uses the kafka idempotent consumer to ensure it is only processed once. 

The Kafka and MongoDB cluster is setup locally using docker and the entire project is developed using python
