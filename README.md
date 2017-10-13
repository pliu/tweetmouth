# Tweetmouth
An information retrieval system that performs semantic and sentiment based analysis on tweets. Ideally, pretty graphs too. Pretty graphs are cool.

## Dependencies
Elasticsearch service running on localhost (port 9300), v5.2
Gradle - as a build system and also to run the application via `gradle run`

## Name
Information retrieval -> Golden Retriever -> original breeder of the Golden retriever: Lord Tweedmouth
The system deals with tweets so Tweedmouth -> Tweetmouth

# Clustering

# Java

### Description
Apache Spark job(s) to perform clustering on tweets.

### Setup
http://www.ics.uci.edu/~shantas/Install_Spark_on_Windows10.pdf

# Python

### Description
Provides a Python script for fetching tweets.

### Setup
Create keys.py in the python folder. This should contain the following variables:
* access_token_key
* access_token_secret
* api_key
* api_secret

These can be obtained through Twitter.

Currently filters out retweets (those that start with 'RT') and tweets containing non-ASCII characters.
