# Twitter Streaming project using Kafka

This project is an experiment learning Apache Kafka.
It takes streams of tweets on a particular topic (trending topic), parses the tweets and loads it into a mongo DB cluster

Architecture
- Tweets are streamed usinf the twitter api 
- The tweets are passed to the Kafka producer topic 
- Tweets are consumed from the Idempotent consumer and loaded to the mongoDB cluster.

It uses the kafka idempotent consumer to ensure it is only processed once. 

The Kafka and MongoDB cluster is setup locally using docker and the entire project is developed using python
