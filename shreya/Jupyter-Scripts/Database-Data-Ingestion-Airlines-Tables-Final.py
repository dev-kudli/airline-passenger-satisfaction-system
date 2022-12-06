#!/usr/bin/env python
# coding: utf-8

# In[289]:


#IMPORTS

from sqlalchemy import create_engine

import re
import pandas as pd
import numpy as np
from plotnine import *
from pandas import DataFrame, read_csv
from wordcloud import WordCloud
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[290]:


#MYSQL CONNECTIONS

mysql_user = "root"
mysql_password = "Shreya1604"
mysql_database = "airline_passenger_satisfaction_system"
mysql_host = "127.0.0.1"
port = 3306

sqlEngine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}',pool_recycle=port)
dbConnection = sqlEngine.connect()


# In[291]:


#FUNCTIONS


def stripChar(x):
    try:
        return re.sub("[()\"]","",x)
    except:
        return np.nan


# In[292]:


#MAIN_FILE INTO DATAFRAME

df_original = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Data/City/airport-codes.csv",header=0,encoding = 'unicode_escape')

#DROP EMPTY ROWS
df_original = df_original.dropna()

#DROP DUPLICATES
df_original = df_original.drop_duplicates(keep='first')

#REPLACE SPACE WITH UNDERSCORE FOR COLUMNS
df_original.columns = df_original.columns.str.replace(' ', '_')

#LOWERCASE ALL THE COLUMN NAMES
df_original.columns = [x.lower() for x in df_original.columns]

#RENAME COLUMN VALUES
df_original.rename(columns={'country_abbrev.':'country_code','airport_code':'iata_airport'}, inplace=True)

#Removing duplicate iata values
#df_original.drop_duplicates(subset = ['iata_airport'], keep = 'first', inplace = True) 
#Removing duplicate iata values
df_original.drop_duplicates(subset = ['country_code','country_name'], keep = 'first', inplace = True) 
df_original.drop_duplicates(subset = ['world_area_code'], keep = 'first', inplace = True)

#df_original


# In[293]:


#INPUT TABLE AIRPORT TABLE

df_airport = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Data/Airport/airport_data_02.txt",header = 0)


#df_airport.loc[df_airport['country_code'].isin('IN')]

# #DROP UNWANTED COLUMNS
# df_airport=df_airport.drop(['url','city','county','state','code'], axis=1)

#RENAMING COLUMN NAMES
df_airport.rename(columns={'region_name':'state','airport':'airport_name','iata':'iata_airport','icao':'icao_airport'}, inplace=True)

# df_airport=df_airport.drop(['url','city','county','state','code'], axis=1)

#Removing duplicate iata values
df_airport.drop_duplicates(subset = ['iata_airport'], keep = 'first', inplace = True) 
df_airport=df_airport.drop_duplicates(keep='first')

df_airport = df_airport[['iata_airport','icao_airport','airport_name','state','country_code','latitude','longitude']]

len(list(df_airport['country_code'].unique()))

#df_airport

#WRITING THE TABLE TO DATABASE
#df_airport.to_sql(con=dbConnection,name='AIRPORT',if_exists='replace',index=False)

#print("Airport table added to database successfully")


# In[294]:


#READING CITY FILE

df_city= pd.read_csv("/Users/shreyajaiswal/Downloads/airport-codes-2.csv",header=0)
df_city = df_city.dropna()
df_city=df_city.rename(columns={'Airport Code':'iata_airport'})
#df_city


# In[295]:


#df_review_merge = merge_df.drop_duplicates(keep='first')
merge_df = pd.merge(df_airport, df_city, on='iata_airport', how='inner')
merge_df = merge_df.drop(['country_code','Airport Name'],axis=1)
merge_df = merge_df.rename(columns={'Country Abbrev.':'country_code','World Area Code':'world_area_code'})
merge_df = merge_df.drop_duplicates(keep='first')
merge_df['area_code_id'] = merge_df['country_code']+merge_df['world_area_code']
merge_df

#DROP EMPTY ROWS
merge_df = merge_df.dropna()

#REPLACE SPACE WITH UNDERSCORE FOR COLUMNS
merge_df.columns = merge_df.columns.str.replace(' ', '_')

#LOWERCASE ALL THE COLUMN NAMES
merge_df.columns = [x.lower() for x in merge_df.columns]

#REMOVE UNWANTED CHARACTERS FROM ROWS
merge_df.country_name = merge_df.country_name.apply(lambda x: stripChar(x))

#merge_df

#len(list(merge_df['iata_airport'].unique()))


# In[296]:


#POPULATE COUNTRY TABLE

df_country= pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Data/Country code/country_code.txt",header=0)

df_country.rename(columns={'Name':'country_name','Code':'country_code'}, inplace=True)


#REPLACE SPACE WITH UNDERSCORE FOR COLUMNS
df_country.columns = df_country.columns.str.replace(' ', '_')

#LOWERCASE ALL THE COLUMN NAMES
df_country.columns = [x.lower() for x in df_country.columns]


#REMOVE UNWANTED CHARACTERS FROM ROWS
df_country.country_name = df_country.country_name.apply(lambda x: stripChar(x))

#Capitalize
df_country.country_name=df_country.country_name.apply(lambda x: x.title())

#REMOVE DUPLICATES
df_country = df_country.drop_duplicates(keep='first')

#Not empty
#df_country = df_country[df_country['country_code'].str.contains('[a-zA-Z]$')]
df_country = df_country.dropna()

df_country

#WRITING THE TABLE TO DATABASE
df_country.to_sql(con=dbConnection,name='country',if_exists='append',index=False)

display(df_country)
print("Country table added to database successfully")


# In[297]:


#POPULATE COUNTRY AREA 

df_country_area = merge_df[['country_code','world_area_code','area_code_id']]
df_country_area = df_country_area.drop_duplicates(keep='first')
#merge_df_ext = merge_df_ext.rename(columns={'Country Abbrev.':'country_code','World Area Code':'world_area_code'})
#df_country['area_code_id'] = df_country.reset_index()

df_country_area = df_country_area[df_country_area['country_code'].str.contains('[a-zA-Z]$')]

# merge_df_ext.reset_index(inplace=True)
# merge_df_ext.index = merge_df_ext.index + 1
# merge_df_ext['area_code_id'] = merge_df_ext.index
# merge_df_ext = merge_df_ext.drop(['index'],axis=1)
# merge_df_ext

#WRITING THE TABLE TO DATABASE
df = pd.merge(df_country,df_country_area,how="left",on=['country_code'])
df = df.dropna(how='any')
df

df_country_area_final = df[['area_code_id','country_code','world_area_code']]


#WRITING THE TABLE TO DATABASE
df_country_area_final.to_sql(con=dbConnection,name='country_area',if_exists='append',index=False)

display(df_country_area_final)
print("COUNTRY_AREA table added to database successfully")
len(list(df['area_code_id'].unique()))


# In[298]:


##POPULATE CITY TABLE

df_city = merge_df[['iata_airport','city_name','country_code']]
df_city = df_city.drop_duplicates(keep='first')
df_city=df_city.rename(columns={'iata_airport':'city_code'})
df_city['city_code'] = df_city['city_code'].str.upper()

df_city = df_city.drop_duplicates(keep='first')
df_city

df_city = df_city[df_city['country_code'].str.contains('[a-zA-Z]$')]


#WRITING THE TABLE TO DATABASE

df_country_code = df_country[['country_code']]
df = pd.merge(df_country_code,df_city,how="left",on=['country_code'])
df.drop_duplicates(subset = ['city_code'], keep = 'first', inplace = True) 
df = df.dropna(how='any')

df = df[['city_code','city_name','country_code']]
df_city_final=df
display(df_city_final)


#WRITING THE TABLE TO DATABASE
df_city_final.to_sql(con=dbConnection,name='city',if_exists='append',index=False)

print("City table added to database successfully")
len(list(df_city_final['city_code'].unique()))


# In[301]:


#POPULATE AIRPORT TABLE

df_airport = merge_df[['iata_airport','icao_airport','airport_name','state','latitude','longitude','area_code_id']]
df_airport = df_airport.drop_duplicates(keep='first')
df_airport
df_airport=df_airport.rename(columns={'iata_airport':'city_code'})

df_city_code = df_city_final[['city_code']]
df = pd.merge(df_city_code,df_airport,how="left",on=['city_code'])
df.drop_duplicates(subset = ['city_code'], keep = 'first', inplace = True) 
df = df.dropna(how='any')
df=df.rename(columns={'city_code':'iata_airport','icao_code':'icao_airport'})
df

# df = df[['city_code','city_name','country_code']]
# df_city_final=df
# display(df_city_final)

#WRITING THE TABLE TO DATABASE
df.to_sql(con=dbConnection,name='airport',if_exists='replace',index=False)

display(df)
print("AIRPORT table added to database successfully")

len(list(df['iata_airport'].unique()))


# In[302]:


#POPULATE AIRLINE TABLE

df_airlines_three = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Data/Airlines/airlines-data-02.csv",header=None, names=['id','name','alias','iata','icao','callsign','country','active'])
df_airlines_three = df_airlines_three.drop([0])
df_airlines_three = df_airlines_three.drop([1])
df_airlines_three = df_airlines_three.drop([2])
df_airlines_three = df_airlines_three.drop([3])

#REMOVING SPACES
df_airlines_three=df_airlines_three.rename(columns=lambda x: x.strip())

#LOWERCASE COLUMN NAMES
df_airlines_three.columns = [x.lower() for x in df_airlines_three.columns]

df_airlines_three=df_airlines_three.drop(['id','alias','callsign'],axis=1)


#RENAME COLUMNS
df_airlines_three.rename(columns={'name':'airline_name','iata':'iata_airline','icao':'icao_airline','country':'country_name'}, inplace=True)

df_airlines_three = df_airlines_three.dropna()

df_airlines_three = df_airlines_three[(df_airlines_three.iata_airline != "\\N")]
df_airlines_three = df_airlines_three[(df_airlines_three.country_name != "\\N")]
df_airlines_three = df_airlines_three[(df_airlines_three.icao_airline != "\\N")]

df_airlines_three = df_airlines_three[df_airlines_three['iata_airline'].str.contains('[a-zA-Z]$')]
df_airlines_three = df_airlines_three[df_airlines_three['icao_airline'].str.contains('[a-zA-Z]$')]

df_airlines_three = df_airlines_three.drop_duplicates(keep='first')
#Removing duplicate iata values
df_airlines_three.drop_duplicates(subset = ['iata_airline'], keep = 'first', inplace = True) 

#df_airlines_three

df_airlines_merge = pd.merge(df_airlines_three, df_country, on='country_name', how='inner') 


# In[303]:


#POPULATE COUNTRY AIRLINE TABLE
df_country_airline = df_airlines_merge[['iata_airline','country_code']]
df_country_airline = df_country_airline.drop_duplicates(keep='first')
df_country_airline['country_airline_id'] = df_country_airline['iata_airline']+df_country_airline['country_code']

df_country_airline = df_country_airline[['country_airline_id','iata_airline','country_code']]
df_country_airline

#WRITING THE TABLE TO DATABASE
df_country_airline.to_sql(con=dbConnection,name='airline_country_detail',if_exists='append',index=False)

display(df_country_airline)
print("AIRLINE_COUNTRY_DETAILS table added to database successfully")


# In[304]:


#POPULATE AIRLINE TABLE

df_airline = df_airlines_merge[['iata_airline','icao_airline','airline_name','active']]
df_airline = df_airline.drop_duplicates(keep='first')
# df_airline


#df_airline=df_airline.drop_duplicates(subset = ['iata_airline'], keep = 'first', inplace = True) 
df_airline_final = df_airline


#WRITING THE TABLE TO DATABASE
df_airline_final.to_sql(con=dbConnection,name='airline',if_exists='append',index=False)

display(df_airline_final)
print("AIRLINES table added to database successfully")
len(list(df_country_airline['country_airline_id'].unique()))


# In[305]:


#AIRLINE REVIEWS

#Reading the Airline CSV files
def readData():
    colnames = ['airline_name','overall_score','review_title','reviewer_country','review_date','aircraft_model','travel_type','seat_type','route','date_flown','seat_comfort_rating','service_rating','food_rating','entertainment_rating','groundservice_rating','wifi_rating','value_rating','recommended']
    Afrance = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/air-france.csv",header = None,names = colnames)
    Ana = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/ana-all-nippon-airways.csv",header = None,names = colnames)
    Emirates = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/emirates.csv",header = None,names = colnames)
    Japan = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/japan-airlines.csv",header = None,names = colnames)
    Korean = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/korean-air.csv",header = None,names = colnames)
    Qantas = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/qantas-airways.csv",header = None,names = colnames)
    Qatar = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/qatar-airways.csv",header = None,names = colnames)
    Singapore = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/singapore-airlines.csv",header = None,names = colnames)
    Swiss = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/swiss-international-air-lines.csv",header = None,names = colnames)
    Turkish = pd.read_csv("/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/turkish-airlines.csv",header = None,names = colnames)
    df = pd.concat([Afrance,Afrance,Ana,Emirates,Japan,Korean,Qantas,Qatar,Singapore,Swiss,Turkish])
    return df


# In[306]:


#READ DATA
df_airline_reviews = readData()
df_airline_reviews

#DROP DUPLICATES
df_airline_reviews = df_airline_reviews.drop_duplicates(keep='first')

#DROP AIRLINE_REVIEWS ROW 0
df_airline_reviews = df_airline_reviews.drop([0])


#STRIP CHARACTERS
df_airline_reviews.review_title = df_airline_reviews.review_title.apply(lambda x: stripChar(x))
df_airline_reviews.reviewer_country = df_airline_reviews.reviewer_country.apply(lambda x: stripChar(x))
#display(df_airline_reviews)


#FILL EMPTY CELLS WITH VAL 0
df_airline_reviews.recommended = df_airline_reviews.recommended.fillna(value = 0)
#display(df_airline_reviews)

#FILL 1 TO CELLS WITH VAL YES
df_airline_reviews.recommended = df_airline_reviews.recommended.eq('yes').mul(1)
#display(df_airline_reviews)

#DATETIME CHANGES
df_airline_reviews['review_date'] = pd.to_datetime(df_airline_reviews['review_date'])
# df_airline_reviews['year'] = df_airline_reviews['review_date'].dt.year
# df_airline_reviews['month'] = df_airline_reviews['review_date'].dt.month
# df_airline_reviews.drop(['date_flown'], axis=1)


display(df_airline_reviews)


# In[268]:


df_new = df_airline_reviews[['airline_name']]
df_new['value']=df_airline_reviews.airline_name.str.lower().isin(list(df_airline_final.airline_name.str.lower()))
df_new=df_new.loc[df_new['value'].isin([True])]
df_new=df_new.drop_duplicates(keep='first')
#df_new

df_airline_final['airline_name'] = df_airline_final['airline_name'].str.lower()
df_airline_final

df_airlines_iata = df_airline_final[['airline_name','iata_airline']]
df_airlines_iata

#MERGING 
merge_df = pd.merge(df_airline_reviews, df_airlines_iata, on='airline_name', how='left')
df_review_main = merge_df
#df_review_main


# In[307]:


##POPULATE AIRLINE_REVIEWS TABLE

df_review_final = df_review_main[['iata_airline','review_title','review_date','seat_comfort_rating','service_rating','food_rating','entertainment_rating','groundservice_rating','wifi_rating','value_rating','overall_score','recommended']]
df_review_final = df_review_final.drop_duplicates(keep='first')
df_review_final


#WRITING THE TABLE TO DATABASE
df_review_final.to_sql(con=dbConnection,name='airline_reviews',if_exists='replace',index=False)

display(df_country_airline)
print("AIRLINE_REVIEWS table added to database successfully")


# In[308]:


df_aircraft = pd.read_csv('/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Data/aircraft/aircraft_types_02.txt',header = 0)
df_aircraft = df_aircraft.drop(['emty','Unnamed: 6'],axis=1)
df_aircraft = df_aircraft.dropna(how='any')
df_aircraft = df_aircraft[df_aircraft.icao_aircraft != 'Nan']
df_aircraft = df_aircraft[df_aircraft.name != 'Nan']
df_aircraft = df_aircraft[df_aircraft.iata_aircraft != 'Nan']
df_aircraft = df_aircraft[df_aircraft.country != 'Nan']
df_aircraft = df_aircraft[df_aircraft.capacity != 'Nan']
df_aircraft = df_aircraft.drop(['country'],axis=1)
len(list(df_aircraft['iata_aircraft'].unique()))
df_aircraft.rename(columns={'name':'aircraft_name','iata_aircraft':'aircraft_iata','icao_aircraft':'aircraft_icao'},inplace=True )
#df_aircraft.country = df_aircraft.country.apply(lambda x: stripChar(x))
df_aircraft = df_aircraft[['aircraft_iata','aircraft_icao','capacity','aircraft_name']]
df_aircraft

#WRITING THE TABLE TO DATABASE
df_aircraft.to_sql(con=dbConnection,name='aircraft',if_exists='append',index=False)

display(df_aircraft)
print("AIRCRAFT table added to database successfully")


# In[ ]:




