from kafka import KafkaProducer
import tweepy
import datetime
import time
import json
import os

client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAADGJjQEAAAAARcp7ktNe8iCaR5kos2huDNXERHM%3Da7JJWmj1Dll9jT5H3X03Mivumyy6u7FWm4Vssdp0utFYoq7iyX')
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#producer = KafkaProducer(security_protocol="PLAINTEXT", bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'))
producer = KafkaProducer(security_protocol="PLAINTEXT", bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'))
producer.flush()
query = 'covid'
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)

while True:
    tweets = client.search_recent_tweets(query=query,
                                        tweet_fields=['context_annotations', 'created_at', 'lang'], 
                                        max_results=100, 
                                        start_time=start_time,
                                        end_time=end_time)
    start_time = end_time
    end_time = start_time + datetime.timedelta(seconds=10)

    for i,tweet in enumerate(tweets.data):
        if tweet.lang == 'en':
            tweet = json.dumps(tweet.text).encode('utf-8')
            producer.send('covid', tweet)
            print(tweet)
    print('Pause de 10 secondes!')
    time.sleep(10)