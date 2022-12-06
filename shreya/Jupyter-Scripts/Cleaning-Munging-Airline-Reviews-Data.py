#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import pandas as pd
import numpy as np
from plotnine import *
from pandas import DataFrame, read_csv
from wordcloud import WordCloud
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


#Reading the Airline CSV files
def readData():
    colnames = ['airline_name','overall_score','review_title','reviewer_country','review_date','aircraft_model','travel_type','seat_type','route','date_flown','seat_comfort_rating','service_rating','food_rating','entertainment_rating','groundservice_rating','wifi_rating','value_rating','recommended']
    Afrance = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/air-france.csv",header = None,names = colnames)
    Ana = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/ana-all-nippon-airways.csv",header = None,names = colnames)
    Emirates = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/emirates.csv",header = None,names = colnames)
    Japan = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/japan-airlines.csv",header = None,names = colnames)
    Korean = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/korean-air.csv",header = None,names = colnames)
    Qantas = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/qantas-airways.csv",header = None,names = colnames)
    Qatar = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/qatar-airways.csv",header = None,names = colnames)
    Singapore = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/singapore-airlines.csv",header = None,names = colnames)
    Swiss = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/swiss-international-air-lines.csv",header = None,names = colnames)
    Turkish = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/turkish-airlines.csv",header = None,names = colnames)
    df = pd.concat([Afrance,Afrance,Ana,Emirates,Japan,Korean,Qantas,Qatar,Singapore,Swiss,Turkish])
    return df


# In[3]:



df_airline_reviews = readData()
display(df_airline_reviews)
print(len(df_airline_reviews))


# In[4]:


df_airline_reviews = df_airline_reviews.drop_duplicates(keep='first')
display(df_airline_reviews)
print(len(df_airline_reviews))


# In[5]:


df_airline_reviews = df_airline_reviews.drop([0])
display(df_airline_reviews)


# In[6]:



def stripChar(x):
    try:
        return re.sub("[()\"]","",x)
    except:
        return np.nan


# In[7]:


print(df_airline_reviews.columns)

#df_top10.ReviewrCountry = df_top10.ReviewrCountry.apply(lambda x: stripChar(x))


# In[8]:


df_airline_reviews.review_title = df_airline_reviews.review_title.apply(lambda x: stripChar(x))
df_airline_reviews.reviewer_country = df_airline_reviews.reviewer_country.apply(lambda x: stripChar(x))


# In[9]:


display(df_airline_reviews)


# In[10]:


df_airline_reviews.recommended = df_airline_reviews.recommended.fillna(value = 0)
display(df_airline_reviews)


# In[11]:


df_airline_reviews.recommended = df_airline_reviews.recommended.eq('yes').mul(1)
display(df_airline_reviews)


# In[13]:


df_airline_reviews.dtypes


# In[23]:


df_airline_reviews['review_date'] = pd.to_datetime(df_airline_reviews['review_date'])
df_airline_reviews['year'] = df_airline_reviews['review_date'].dt.year
df_airline_reviews['month'] = df_airline_reviews['review_date'].dt.month
df_airline_reviews.drop(['date_flown'], axis=1)

df_airline_reviews.to_csv(f'/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/reviews.csv', mode='a', header=True, index=False)


# In[ ]:




