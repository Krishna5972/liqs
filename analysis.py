import asyncio
import os
import json
import pandas as pd
from websockets import connect
from datetime import datetime,date,timedelta
import ccxt
import pandas as pd
from binance.client import Client
import math
import time
from datetime import datetime
from data_extraction import *
import websocket
import requests
import time
import pytz
from functions import *

ist_timezone = pytz.timezone('Asia/Kolkata')

api_key='xJpvqShFcw4mp18Unfq6Xg9FOWMGvcrJDIPGqAl252JC2renwE3wBmF6N3whiooz'
secret_key='RKCkXXEhfLSXHUVHMeLHChlULX4pr6ksae57OQ6UbCMXyyBDnO9Vca5PaxuvKw6r'

exchange = ccxt.binance({
    "apiKey": api_key,
    "secret": secret_key,
    'options': {
    'defaultType': 'future',
    },
})

client=Client(api_key,secret_key)

df=pd.read_csv('liqs.csv')
df=df[~df['s'].isin(['ETHUSDT','ETHBUSD','BTCUSDT','BTCBUSD'])]
df.reset_index(inplace=True)

df['time']=pd.to_datetime(df['time'])
df['day']=df['time'].dt.day
df['month']=df['time'].dt.month
df['hour']=df['time'].dt.hour
df['minute']=df['time'].dt.minute
df['year']=df['time'].dt.year
df['second']=df['time'].dt.second

for idx,row in df.iterrows():
    if idx%2000==0:
        mail_df=df.copy()
        mail_df.dropna(inplace=True)
        mail_df.to_csv('analysis.csv',mode='w+',index=False)
        send_mail('analysis.csv')

    try:
        print(row['s'])
        coin = row['s']       
        date_object_start = date(row['year'], row['month'], row['day'])
        date_object_end = date_object_start+timedelta(days=10)
        date_object_start = date_object_start-timedelta(days=1)
        print(date_object_start)
        str_date = date_object_start.strftime("%b %d, %Y")
        end_str = date_object_end.strftime("%b %d, %Y")
        print(f'Retreving from {str_date} to {end_str}')
        print(str_date) 
        timeframe='1m'
        print(coin)
        new_df=dataextract(coin,str_date,end_str,timeframe,client)
        new_df_index=new_df[(new_df['hour']==row['hour']) & (new_df['minute']==row['minute']) \
              & (new_df['day']==row['day']) & (new_df['month']==row['month']) & (new_df['year']==row['year'])].index[0]
        new_df=new_df[new_df.index > new_df_index]



        if row['second'] >= 30:
            entry=new_df.iloc[0]['close']
        else:   
            entry=new_df.iloc[0]['open']
        max_price=max(new_df['high'])
        min_price=min(new_df['low'])
        max_perc=round((max_price-entry)/entry,4)
        min_perc=round((entry-min_price)/entry,4)
        df.at[idx,'entry']=entry
        df.at[idx,'min_price']=min_price
        df.at[idx,'max_price']=max_price
        df.at[idx,'max_perc']=max_perc
        df.at[idx,'min_perc']=min_perc

    
        time.sleep(10)
    except Exception as e:     
        client=Client(api_key,secret_key)
        print('Exception')
        time.sleep(60)
        print(row['s'])
        coin = row['s']       
        date_object_start = date(row['year'], row['month'], row['day'])
        date_object_end = date_object_start+timedelta(days=10)
        date_object_start = date_object_start-timedelta(days=1)
        print(date_object_start)
        str_date = date_object_start.strftime("%b %d, %Y")
        end_str = date_object_end.strftime("%b %d, %Y")
        print(f'Retreving from {str_date} to {end_str}')
        print(str_date) 
        timeframe='1m'
        print(coin)
        new_df=dataextract(coin,str_date,end_str,timeframe,client)
        new_df_index=new_df[(new_df['hour']==row['hour']) & (new_df['minute']==row['minute']) \
              & (new_df['day']==row['day']) & (new_df['month']==row['month']) & (new_df['year']==row['year'])].index[0]
        new_df=new_df[new_df.index > new_df_index]



        if row['second'] >= 30:
            entry=new_df.iloc[0]['close']
        else:   
            entry=new_df.iloc[0]['open']
        max_price=max(new_df['high'])
        min_price=min(new_df['low'])
        max_perc=round((max_price-entry)/entry,4)
        min_perc=round((entry-min_price)/entry,4)
        df.at[idx,'entry']=entry
        df.at[idx,'min_price']=min_price
        df.at[idx,'max_price']=max_price
        df.at[idx,'max_perc']=max_perc
        df.at[idx,'min_perc']=min_perc

        
        time.sleep(10)

