import asyncio
import os
import json
import pandas as pd
from websockets import connect
from datetime import datetime
import ccxt
import pandas as pd
from binance.client import Client
import math
import time
from datetime import datetime
import websocket
import requests
import time
import pytz
from functions import *
ist_timezone = pytz.timezone('Asia/Kolkata')

api_key='8bb16efb8d13fc1542c86e9cb104808feed4c054cc8111caa5645f7aaf3e681f'
secret_key='35a48f605502ee5871cd50d4e109e16cf9be105e5b64748673492403c40d64d7'

exchange = ccxt.binanceus({
    "apiKey": api_key,
    "secret": secret_key,
    'options': {
    'defaultType': 'future',
    },
})

client=Client(api_key,secret_key)

url='wss://fstream.binance.com/ws/!forceOrder@arr'
import ccxt
from pprint import pprint
print('CCXT Version:', ccxt.__version__)
exchange = ccxt.binance({
    'enableRateLimit': True,
})
response = exchange.fapiPublicGetPremiumIndex()


import requests
api_key='xJpvqShFcw4mp18Unfq6Xg9FOWMGvcrJDIPGqAl252JC2renwE3wBmF6N3whiooz'
def funding_rate(x,response):
    for i in range(0,len(response)):
        if response[i]['symbol'] == x:
            return response[i]['lastFundingRate']
        
log_df=pd.read_csv('liqs.csv')
master_df=pd.DataFrame()
i=0
ws = websocket.WebSocket()
mail_counter=0
# Connect to the WebSocket
ws.connect(url)
while True:
    msg=ws.recv()
    dict_=json.loads(msg)['o']
    df=pd.DataFrame([dict_])
    master_df=pd.concat([master_df,df])
    i+=1
    print(i)
    master_df['q']=master_df['q'].astype('float64')
    master_df['ap']=master_df['ap'].astype('float64')
    master_df['liq_amount']=master_df['q']*master_df['ap']
    final=master_df.groupby('s').agg({'liq_amount':'sum','S':'max','q':'count'}).reset_index()
    final['funding_rate']=final['s'].apply(funding_rate,response=response)
    final['funding_rate']=final['funding_rate'].astype('float64')
    final['funding_rate']*=100
    final['funding_rate']=round(final['funding_rate'],4)
    final['time']=datetime.now(ist_timezone)
    final.sort_values(by=['liq_amount','funding_rate'],ascending=False,inplace=True)
    if max(final['liq_amount'])>30000:
        amount_symbol_dict=dict(list(zip(final['liq_amount'],final['s'])))
        print(amount_symbol_dict[max(final['liq_amount'])])
        symbol=amount_symbol_dict[max(final['liq_amount'])]
        final=final[final['liq_amount']>30000]
        symbols=final['s'].to_list()
        print(symbols)
        log_df=pd.concat([log_df,final])
        print(master_df)
        master_df=master_df[~master_df['s'].isin(symbols)]
        print(master_df)
        log_df.to_csv('liqs.csv',index=False,mode='w+')
        mail_counter+=1
        if mail_counter > 20:  #send mail for every new 10 coins
            send_mail('liqs.csv')
            mail_counter=0
        
        