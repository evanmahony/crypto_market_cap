# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 19:30:46 2020

@author: Evan
"""

import datetime as dt
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import os
import time

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

parameters = {
  'start': '1',
  'limit': '200',
  'convert': 'EUR'
}

f = open('header.json')
headers = json.load(f)
f.close()

def directory_check():
    if not os.path.exists('data'):
        os.mkdir('data')
        
    if not os.path.exists('data\quote'):
        os.mkdir('data\quote')
        
    if not os.path.exists('data\overview'):
        os.mkdir('data\overview')
    
def data_request(url, parameters, headers):
    session = Session()
    session.headers.update(headers)
    
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        # Need to print a bit more data, successful at:
        print('Successful')
        return data
      
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
  
def data_clean(data):
    df = pd.DataFrame(data['data'])
    df.set_index(['id'], inplace = True )
    
    for i in df.index:
        if 'stablecoin' in df.loc[i]['tags']:
            df.drop(i, inplace=True)
            
    date_object = dt.datetime.strptime(df.loc[1]['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ')
    date_string = date_object.strftime("%d_%m_%y")
  
    return df, date_object, date_string

def overview_df(df, date_string):
    # Overview Data for the day
    
    if not os.path.exists('data\overview\\' + date_string + '.csv'):  
        overview_df = df[df.columns[:7]].join(df[df.columns[9:12]])
        overview_df.to_csv('data\overview\\' + date_string + '.csv')

def quote(df, date_string, quote_df=pd.DataFrame()):
    quote_data = pd.DataFrame()
    
    for i in df.index:
        if quote_data.empty:
            quote_data = pd.DataFrame.from_dict([df['quote'].loc[i]['EUR']])
            
        else:
            quote_data = quote_data.append(pd.DataFrame.from_dict([df['quote'].loc[i]['EUR']]))
            
    quote_data.set_index([df.index, pd.to_datetime(quote_data.loc[:]['last_updated'])], inplace=True)
    quote_data.drop(columns='last_updated', inplace=True)
    
    # Need to create these directories if they don't exist
    if quote_df.empty or not os.path.exists('data\quote\\' + date_string + '.csv'):
        quote_df = quote_data
    
    else:
        quote_df = quote_df.append(quote_data)
        
    quote_df.to_csv('data\quote\\' + date_string + '.csv')
    return quote_df, quote_data

directory_check()

data1 = data_request(url, parameters, headers)

df1, date_object1, date_string1 = data_clean(data1)

overview_df(df1, date_string1)
quote_df1, quote_data1 = quote(df1, date_string1)

time.sleep(600)

data2 = data_request(url, parameters, headers)

df2, date_object2, date_string2 = data_clean(data2)

overview_df(df2, date_string2)
quote_df2, quote_data2 = quote(df2, date_string2, quote_df1)