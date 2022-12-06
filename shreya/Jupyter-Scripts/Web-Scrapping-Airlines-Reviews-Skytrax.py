#!/usr/bin/env python
# coding: utf-8

# In[1]:


import scrapy
import scrapy.crawler as crawler
from multiprocessing import Process, Queue
from twisted.internet import reactor
from scrapy import Spider
import re

from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess

import scrapy.crawler as crawler
from scrapy.utils.log import configure_logging
from multiprocessing import Process, Queue
from twisted.internet import reactor

import scrapy
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging


# In[2]:


#Import needed libraries
from bs4 import BeautifulSoup 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import requests 
import pygal
import time
import pygal
from datetime import datetime
import re
import pandas as pd


# In[3]:


from ipywidgets import widgets

#print(airline_name.value)

columns_dropdown=widgets.Dropdown(
    options=['emirates','qatar-airways','qantas-airways','singapore-airlines','ana-all-nippon-airways','japan-airlines','turkish-airlines','air-france','korean-air','swiss-international-air-lines','british-airways','lufthansa'],
    value='emirates',
    description='Number:',
    disabled=False,
)

display(columns_dropdown)


# In[4]:


airline_name = columns_dropdown.value
print(airline_name)


# In[5]:


from scrapy import Spider
#from Top10Air.items import Top10AirItem
import re


class TopTenAirSpider(Spider):
    
    print("\nTopTenAirSpider BEING EXECUTED.............\n")
    print(airline_name)
    name = "Top10AirSpider"
    df = pd.DataFrame(columns=['airline_name','overall_score','review_title','reviewer_country','review_date','aircraft_model','travel_type','seat_type','route','date_flown','seat_comfort_rating','service_rating','food_rating','entertainment_rating','groundservice_rating','wifi_rating','value_rating','recommended'])    
    df.to_csv(f'/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Top_Ten_Airlines/{airline_name}.csv', mode='w', header=True, index=False)

    
    
    allowed_urls = ['https://www.airlinequality.com/']
    start_urls = [f'https://www.airlinequality.com/airline-reviews/{airline_name}/page/' + str(i) for i in range(100)]
    
    def parse(self, response):
        data=[]
        reviews = response.xpath('//article[@itemprop="review"]')
        for review in reviews:
            #print("REVIEW NUMBER --> ", idx+1)
            record = {}
            airline = airline_name.replace('-',' ')
            record['airline_name'] = airline
            #OverallScore
            

            try:
                OverallScore = review.xpath('./div/span[1]/text()').extract_first()
                record['overall_score'] = OverallScore
            except:
                OverallScore = ''
                record['overall_score'] = OverallScore


            #ReviewTitle
            try:
                ReviewTitle = review.xpath('././div/h2/text()').extract_first()
                record['review_title'] = ReviewTitle
            except:
                ReviewTitle = ''
                record['review_title'] = ReviewTitle


            #ReviewrCountry
            try:
                ReviewrCountry = response.xpath('//article[@itemprop="review"]/div/h3/text()').extract()[1]
                record['reviewer_country'] = ReviewrCountry
            except:
                ReviewrCountry = ''
                record['reviewer_country'] = ReviewrCountry


            #ReviewDate
            try:
                ReviewDate = review.xpath('././div/h3/time/text()').extract_first()
                record['review_date'] = ReviewDate
            except:
                ReviewDate = ''
                record['review_date'] = ReviewDate


            #AircraftModel
            try:
                AircraftModel = review.xpath('.//td[@class="review-rating-header aircraft "]/../td[2]/text()').extract_first()
                record['aircraft_model'] = AircraftModel
            except:
                AircraftModel = ''
                record['aircraft_model'] = AircraftModel

            #TravelType
            try:
                TravelType = review.xpath('.//td[@class="review-rating-header type_of_traveller "]/../td[2]/text()').extract_first()
                record['travel_type'] = TravelType
            except:
                TravelType = ''
                record['travel_type'] = TravelType

            #SeatType
            try:
                SeatType = review.xpath('.//td[@class="review-rating-header cabin_flown "]/../td[2]/text()').extract_first()
                record['seat_type'] = SeatType
            except:
                SeatType = ''
                record['seat_type'] = SeatType

            #Route
            try:
                Route = review.xpath('.//td[@class="review-rating-header route "]/../td[2]/text()').extract_first()
                record['route'] = Route
            except:
                Route = ''
                record['route'] = Route

            #DateFlown
            try:
                DateFlown = review.xpath('.//td[@class="review-rating-header date_flown "]/../td[2]/text()').extract_first()
                record['date_flown'] = DateFlown
            except:
                DateFlown = ''
                record['date_flown'] = DateFlown

            #SeatComfortRating
            try:
                SeatComfortRatingPrep = review.xpath('.//td[@class="review-rating-header seat_comfort"]/../td[2]/span/@class').extract()
                SeatComfortRating = SeatComfortRatingPrep.count('star fill')
                record['seat_comfort_rating'] = SeatComfortRating
            except:
                SeatComfortRating = ''
                record['seat_comfort_rating'] = SeatComfortRating

            #ServiceRating
            try:
                ServiceRatingPrep = review.xpath('.//td[@class="review-rating-header cabin_staff_service"]/../td[2]/span/@class').extract()
                ServiceRating = ServiceRatingPrep.count('star fill')
                record['service_rating'] = ServiceRating
            except:
                ServiceRating = ''
                record['service_rating'] = ServiceRating

            #FoodRating
            try:
                FoodRatingPrep = review.xpath('.//td[@class="review-rating-header food_and_beverages"]/../td[2]/span/@class').extract()
                FoodRating = FoodRatingPrep.count('star fill')
                record['food_rating'] = FoodRating
            except:
                FoodRating = ''
                record['food_rating'] = FoodRating

            #EntertainmentRating
            try:
                EntertainmentRatingPrep = review.xpath('.//td[@class="review-rating-header inflight_entertainment"]/../td[2]/span/@class').extract()
                EntertainmentRating = EntertainmentRatingPrep.count('star fill')
                record['entertainment_rating'] = EntertainmentRating
            except:
                EntertainmentRating = ''
                record['entertainment_rating'] = EntertainmentRating

            #GroundServiceRating
            try:
                GroundServiceRatingPrep = review.xpath('.//td[@class="review-rating-header ground_service"]/../td[2]/span/@class').extract()
                GroundServiceRating = GroundServiceRatingPrep.count('star fill')
                record['groundservice_rating'] = GroundServiceRating
            except:
                GroundServiceRating = ''
                record['groundservice_rating'] = GroundServiceRating

            #WifiRating
            try:
                WifiRatingPrep = review.xpath('.//td[@class="review-rating-header wifi_and_connectivity"]/../td[2]/span/@class').extract()
                WifiRating = WifiRatingPrep.count('star fill')
                record['wifi_rating'] = WifiRating
            except:
                WifiRating = ''
                record['wifi_rating'] = WifiRating

            #ValueRating
            try:
                ValueRatingPrep = review.xpath('.//td[@class="review-rating-header value_for_money"]/../td[2]/span/@class').extract()
                ValueRating = ValueRatingPrep.count('star fill')
                record['value_rating'] = ValueRating
            except:
                ValueRating = ''
                record['value_rating'] = ValueRating

            #Recommended
            try:
                Recommended = review.xpath('.//td[@class="review-value rating-yes"]/text()').extract_first()
                record['recommended'] = Recommended
            except:
                Recommended = 'no'
                record['recommended'] = Recommended

            
            data.append(record)
            
        df = pd.DataFrame(data)
        display(df)
        df.to_csv(f'/Users/shreyajaiswal/Desktop/Shreya/Semester_one/DMDD/Assignment 3 /Web_Scrapping_Data/Airlines_Data/{airline_name}.csv', mode='a', header=False, index=False)
        print("CSV FILE WRITTEN ---> RECORD COUNT --> ", len(df),"\n")   

    #print("OUTSIDE THE FUNC")


# In[6]:


process = CrawlerProcess()
process.crawl(TopTenAirSpider)
process.start()
#https://www.airlinequality.com/airline-reviews/singapore-airlines


# In[ ]:




