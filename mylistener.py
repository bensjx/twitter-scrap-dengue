import re
import tweepy
import pandas as pd
from textblob import TextBlob
from googletrans import Translator
import psycopg2
import parameters
import os


# Override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):

        """ Ran everytime a new tweet comes in """

        if status.retweeted:
            # Avoid retweeted info, and only original tweets will be received
            return True

        # Extract attributes from each tweet
        id_str = status.id_str
        created_at = status.created_at
        clean_text, text = preprocess(status.text)  # Pre-processing the text
        sentiment = TextBlob(text).sentiment
        polarity = sentiment.polarity

        mycursor = conn.cursor()

        # # Clean up database: delete data older than 10 days
        # delete_query = "DELETE FROM {0} WHERE created_at < NOW() - INTERVAL 10 DAY;".format(
        #     "dengue"
        # )

        delete_query = "DELETE FROM {} WHERE created_at < (now() - interval '7days')".format(
            "dengue"
        )

        mycursor.execute(delete_query)
        conn.commit()

        # Insert new data into database
        sql = "INSERT INTO {} (id_str, created_at, clean_text, text, polarity) VALUES (%s, %s, %s, %s, %s)".format(
            parameters.TABLE_NAME
        )
        val = (id_str, created_at, clean_text, text, polarity)
        mycursor.execute(sql, val)
        conn.commit()
        mycursor.close()

        # # Store all data in MySQL
        # if mydb.is_connected():
        #     mycursor = mydb.cursor()
        #     sql = "INSERT INTO {} (id_str, created_at, clean_text, text, polarity) VALUES (%s, %s, %s, %s, %s)".format(
        #         parameters.TABLE_NAME
        #     )
        #     val = (id_str, created_at, clean_text, text, polarity)
        #     mycursor.execute(sql, val)
        #     mydb.commit()
        #     # mycursor.close()
        #     # print(sql)

        #     # Clean up database: delete data older than 10 days
        #     delete_query = "DELETE FROM {0} WHERE created_at < NOW() - INTERVAL 10 DAY;".format(
        #         "dengue"
        #     )

        #     mycursor.execute(delete_query)
        #     mydb.commit()
        #     mycursor.close()

    def on_error(self, status_code):
        """
        Since Twitter API has rate limits, stop srcraping data as it exceed to the thresold.
        """
        if status_code == 420:
            # return False to disconnect the stream
            return False


def preprocess(text):
    if text:
        # Translate to english
        translator = Translator()
        translated_text = translator.translate(text, dest="en").text

        # Remove emoji
        translated_emojiless_text = translated_text.encode("ascii", "ignore").decode(
            "ascii"
        )

        # Remove links
        translated_emojiless_clean_text = " ".join(
            re.sub(
                "(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(RT)|(\()|(\))|,|\.|:|;",
                " ",
                translated_emojiless_text,
            ).split()
        )

        clean_text = "".join(
            re.sub(
                "(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", translated_text
            )
        )

        return clean_text, translated_emojiless_clean_text
    else:
        return None


DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cur = conn.cursor()

cur.execute(
    """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(
        parameters.TABLE_NAME
    )
)
if cur.fetchone()[0] == 0:
    cur.execute(
        "CREATE TABLE {} ({});".format(
            parameters.TABLE_NAME, parameters.TABLE_ATTRIBUTES
        )
    )
    conn.commit()
cur.close()


# # Initialise MySQL
# try:
#     mydb = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         passwd="password",
#         database="TwitterDB",
#         charset="utf8",
#     )
# except:
#     db1 = mysql.connector.connect(host="localhost", user="root", passwd="password")
#     cursor = db1.cursor()
#     sql = "CREATE DATABASE TwitterDB"
#     cursor.execute(sql)
#     mydb = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         passwd="password",
#         database="TwitterDB",
#         charset="utf8",
#     )

# # Setup DB
# if mydb.is_connected():
#     mycursor = mydb.cursor()
#     mycursor.execute(
#         """
#             SELECT COUNT(*)
#             FROM information_schema.tables
#             WHERE table_name = '{0}'
#             """.format(
#             parameters.TABLE_NAME
#         )
#     )
#     if mycursor.fetchone()[0] == 0:
#         mycursor.execute(
#             "CREATE TABLE {} ({});".format(
#                 parameters.TABLE_NAME, parameters.TABLE_ATTRIBUTES
#             )
#         )
#         mydb.commit()
#         mycursor.close()


# Initialise tweepy
try:  # local
    from keys import *

    auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
    auth.set_access_token(twitter_access_token, twitter_access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
except:  # heroku
    auth = tweepy.OAuthHandler(
        os.environ["twitter_consumer_key"], os.environ["twitter_consumer_secret"]
    )
    auth.set_access_token(
        os.environ["twitter_access_token"], os.environ["twitter_access_token_secret"],
    )
    api = tweepy.API(auth, wait_on_rate_limit=True)


# Start streaming. This will always run on Heroku along wtih app.py
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(track=parameters.TRACK_WORDS)
