from pymongo import MongoClient
import urllib.parse
import contextlib

from dotenv import load_dotenv
import os
load_dotenv()

DB_USER = os.environ.get('DB_USER')
DB_PWD = os.environ.get('DB_PWD')

 
USERNAME = urllib.parse.quote_plus(DB_USER)
PASSWORD = urllib.parse.quote_plus(DB_PWD)
CONN_STRING = f"mongodb://{USERNAME}:{PASSWORD}@mongo1:30001/?authMechanism=DEFAULT&replicaSet=mongoRS01&directConnection=true"

print(CONN_STRING)

@contextlib.contextmanager
def get_mongo_conn():
    """
    Context manager to automatically close DB connection. 
    We retrieve credentials from Environment variables
    """
    try:
        conn = MongoClient(CONN_STRING)
        if conn != None:
            print(f"Database connected {conn.server_info}")
            yield conn
    except Exception as e:
        print(e)
    finally:
        conn.close()

def setup_database():
    with get_mongo_conn() as client:
        try:
            if client != None:
                db_names = client.list_database_names()
                print(db_names)

                if 'tweet_stream' not in db_names:
                    tweet_stream_db = client['tweet_stream']
                    tweet = {
                        "data": {
                            "id": "1569395234148159493", 
                            "text": "RT @DrJeffKwong: seeking a keen postdoctoral fellow (for up to 2 yrs) to help lead COVID and MPX vaccine surveillance (coverage, effectiven\\u2026"
                        }, 
                        "matching_rules": [{"id": "1568641967768178689", "tag": "databases pictures"}]
                    }

                    twitter_stream = tweet_stream_db['twitter_stream']
                    inserted_doc = twitter_stream.insert_one(tweet)
                    print(inserted_doc.inserted_id)
                else:
                    pass
        except Exception as e:
            print(e)

def insert_stream(db_conn, db_name = 'tweet_stream', collection_name = 'twitter_stream', tweet=None):
    if tweet == None:
        print("cannot insert empty tweet")
        return None
    else:
        try:
            tweet_stream_db = db_conn[db_name]
            tweet_stream_col = tweet_stream_db[collection_name]
            inserted_tweet = tweet_stream_col.insert_one(tweet)
            return ( inserted_tweet.inserted_id)
        except Exception as e:
            print(e)

def insert_stream_batch(db_conn, db_name = 'tweet_stream', collection_name = 'twitter_stream', tweets=None):
    if tweets == None:
        print("cannot insert empty batch")
        return None
    if type(tweets) != list:
        print("Tweets must be an array")
        return "Tweets must be an array"
    else:
        try:
            tweet_stream_db = db_conn[db_name]
            tweet_stream_col = tweet_stream_db[collection_name]
            inserted_tweets = tweet_stream_col.insert_many(tweets, ordered=True)
            return ( inserted_tweets.inserted_ids)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    # setup_database()
    sample_tweet = {"data": {"id": "1569790160274866180", "text": "@Gerjon_ #Tigray army head confirmed today #Ethiopian airlines has been transporting weapons &amp; soldiers to Asmara, &amp; Massawa, Eritrea."}, "matching_rules": [{"id": "1568641967768178689", "tag": "databases pictures"}]}
    with get_mongo_conn() as db_connection:
        insert_stream(db_connection, tweet=sample_tweet)

