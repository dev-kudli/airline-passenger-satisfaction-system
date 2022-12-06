#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import snscrape.modules.twitter as sntwitter
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer

import string
import re
import textblob
from textblob import TextBlob

from wordcloud import WordCloud, STOPWORDS
from emot.emo_unicode import UNICODE_EMOJI

lemmatizer = WordNetLemmatizer()
from wordcloud import ImageColorGenerator
from PIL import Image

import warnings
get_ipython().run_line_magic('matplotlib', 'inline')

from sqlalchemy import create_engine
import pymysql


# In[6]:


#MYSQL CONNECTIONS

mysql_user = "root"
mysql_password = "Shreya1604"
mysql_database = "airline_passenger_satisfaction"
mysql_host = "127.0.0.1"
port = 3306

sqlEngine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}',pool_recycle=port)
dbConnection = sqlEngine.connect()


# In[7]:


#FIND AIRLINE NAMES FROM TWITTER TEXT

def airlineName(tweet):
        text = (tweet.content).lower()
        print(f"\ntweet id --> {text}")
        print(type(text))
        airline = [];
        
        
        
        if((("@qatar" in text) or ("Qatar") in text) and ("qatar") not in airline):
            airline.append("Qatar")
        elif(("@british" in text) and ("british_airways") not in airline):
            airline.append("British_airways")
        elif(("@lufthanza" in text) and ("lufthanza") not in airline):
            airline.append("Lufthanza")  
        elif(("@lemirates" in text) and ("emirates") not in airline):
            airline.append("Emirates")  
        elif(("@singapore" in text) and ("singapore_airways") not in airline):
            airline.append("singapore_airways")  
        elif(("@gofirstairways" in text) and ("gofirstairways") not in airline):
            airline.append("GoFirstairways")
        elif(("@turkish_airways" in text) and ("Turkish_Airways") not in airline):
            airline.append("Turkish_Airways")
        elif(("@american_air" in text) and ("american_air") not in airline):
            airline.append("American_Air")
        elif(("@djsaviation" in text) and ("djsaviation") not in airline):
            airline.append("DjsAviation")
        
            
        airline_str = ''.join(str(x) for x in airline)
        return airline_str


# In[9]:


#IITERATE THROUGH TWEETS USING SNSCRAPE

keywords =  "(airlines or airline or flight or airways)"
#keywords =  "(airfrance)"
tweet_user = []
tweets = []
tweet_tags = []
tweet_mentions = []
airlines = []

tweet_airline = []

for i, tweet in enumerate(sntwitter.TwitterSearchScraper(keywords).get_items()):

    if i >= 1000:
        break
    else:
        airline_name = airlineName(tweet)
        tweet_id_str = (str)(tweet.id);
        print(type(tweet.date))
        print(f"tweet id --> {tweet.user.id}")
        
        tweets.append([tweet.id,tweet.user.username,tweet.content,tweet.user.profileImageUrl,tweet.likeCount,tweet.date])
        tweet_user.append([tweet.user.username,tweet.user.displayname,tweet.user.id,tweet.user.profileImageUrl,tweet.user.description, tweet.user.followersCount, tweet.user.friendsCount])
       
        tweet_tags.append([tweet.id,tweet.hashtags])
        tweet_mentions.append([tweet.id,tweet.user.username,tweet.mentionedUsers])
        airlines.append([tweet.id,airline_name,tweet.date,tweet.user.location])
        


# In[4]:


df1 = pd.read_sql('SELECT * FROM TWEETS where twitter_handle = "eurocontrolDG" ', con=dbConnection)
display(df1)


# In[11]:


#TABLE TWEETS

cols_tweet = ["tweet_id","twitter_handle","tweet_text","profile_image_url","like_count","created_at"]
df_tweets = pd.DataFrame(tweets, columns = cols_tweet)

#DROPPING DUPLICATES
df_tweets=df_tweets.drop_duplicates('twitter_handle')
print("TWEETS TABLE GENERATED FROM SNSCRAPE")
df_tweets=df_tweets.sort_values("twitter_handle")
display(df_tweets)

#READING TWITTER TABLE FROM MYSQL
df1 = pd.read_sql('SELECT * FROM TWEETS ', con=dbConnection)
df1=df1.sort_values("twitter_handle")
print("TWITTER TABLE IN DATABASE")
display(df1)

#EXTRACTING THE TWITTER_HANDLE COLUMN VALUES
twitter_handle_list = df1['twitter_handle'].tolist()
#print(col_one_list)

#REMOVING THE RECORDS OF ALREADY EXISTING TWITTER HANDLE
df_final = df_tweets[~df_tweets.twitter_handle.isin(twitter_handle_list)]
df_final=df_final.sort_values("twitter_handle")

print("FINAL TABLE TO BE INSERTED INTO THE DATABASE")
display(df_final)


#SORTING
#df_tweets.sort_values("twitter_handle")
#display(df_tweets)


result=df_tweets.dtypes
print(result)

#APPENDING DATA TO MYSQL DATABASE
df_final.to_sql(con=dbConnection,name='TWEETS',if_exists='replace',index=False)

print("SUCCESSFULLY ADDED")


# In[12]:


#TABLE TWEET_USER
cols_tweet_user = ["twitter_handle","user_dispay_name","user_id","user_profile_image_url","user_description","user_followers_count","user_following_count"]
df_tweets_user = pd.DataFrame(tweet_user, columns = cols_tweet_user)

#df_tweets_user.sort_values("twitter_handle")
display(df_tweets_user)

#DROPPING DUPLICATES
df_tweets_user=df_tweets_user.drop_duplicates('twitter_handle')

result=df_tweets_user.dtypes
print(result)

#APPENDING DATA TO MYSQL DATABASE
df_tweets_user.to_sql(con=dbConnection,name='USER',if_exists='replace',index=False)

print("SUCCESSFULLY ADDED")


# In[13]:


#TABLE TWEET TAGS

cols_tweet_tags = ["tweet_id","hashtags"]
df_tweets_tags = pd.DataFrame(tweet_tags, columns = cols_tweet_tags)
display(df_tweets_tags)

result=df_tweets_tags.dtypes
print(result)

df_tweets_tags['hashtags'] = df_tweets_tags['hashtags'].astype(str)
result=df_tweets_tags.dtypes
print(result)

#APPENDING DATA TO MYSQL DATABASE
df_tweets_tags.to_sql(con=dbConnection,name='TWEET_TAGS',if_exists='replace',index=False)

print("SUCCESSFULLY ADDED")


# In[14]:


#TABLE MENTIONS

cols_tweet_mentions = ["tweet_id","source_user","target_user"]
df_tweets_mentions = pd.DataFrame(tweet_mentions, columns = cols_tweet_mentions)
display(df_tweets_mentions)

result=df_tweets_mentions.dtypes
print(result)

#df_tweets_mentions['source_user'] = df_tweets_mentions['source_user'].astype("|S")
df_tweets_mentions['target_user'] = df_tweets_mentions['target_user'].astype(str)
result=df_tweets_mentions.dtypes
print(result)

#APPENDING DATA TO MYSQL DATABASE
df_tweets_mentions.to_sql(con=dbConnection,name='TWEET_MENTIONS',if_exists='replace',index=False)

print("SUCCESSFULLY ADDED")


# In[15]:


#AIRLINE TABLE

cols_airlines = ["tweet_id","airline_name","created_at","user_location"]
df_airlines = pd.DataFrame(airlines, columns = cols_airlines)
display(df_airlines)

df_airlines.to_sql(con=dbConnection,name='AIRLINES',if_exists='replace',index=False)

print("SUCCESSFULLY ADDED")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




