"""
What would be the cost of holding a short (a long) position per x amount of time? 
How to estimate that cost with a constantly changing position?  (Current cost of hedging)

Pull spot hourly along with quarterly hourly to get synthetic quarterly basis

BTC
"""

import pandas as pd
import numpy as np
import requests
import json
import datetime as dt
import itertools
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

def get_perp_funding(ticker):
    perp_rates = 'https://ftx.com/api/funding_rates'
    temp_funding = []
    url = perp_rates + '?future=' + ticker
    r = requests.get(url)
    funding_temp = json.loads(r.content)['result']
    temp_funding.append(funding_temp)
    unix_end = int(pd.to_datetime(funding_temp[-1]['time']).timestamp())
    while len(funding_temp)>1:
        url_chain = url + '&end_time=' + str(unix_end)
        r = requests.get(url_chain)
        funding_temp = json.loads(r.content)['result']
        temp_funding.append(funding_temp)
        unix_end = int(pd.to_datetime(funding_temp[-1]['time']).timestamp())
    temp_funding = list(itertools.chain(*temp_funding))
    funding_frame = pd.DataFrame(temp_funding)
    funding_frame.drop_duplicates(inplace=True)
    funding_frame.set_index('time',inplace=True)
    funding_frame.index = pd.to_datetime(funding_frame.index)
    funding_frame = funding_frame.resample('1h').last().fillna(method='ffill')
    funding_frame.rename({'rate':'Perp'},axis=1,inplace=True)
    return funding_frame

def get_historical_price(ticker, index=False, time = None, resolution = 3600):
    '''hourly candles'''
    temp_price = []
    
    if index:
        url = 'https://ftx.com/api/indexes/'+ticker +'/candles?resolution=' + str(resolution)
        url_base = url
    else:
        url_base = 'https://ftx.com/api/markets/'+ticker +'/candles?resolution=' + str(resolution)
        if time is not None:
            current_time = int(dt.datetime.utcnow().timestamp()) 
            if int(pd.to_datetime(time).timestamp())<current_time:
                url = url_base + '&end_time='+str(int(pd.to_datetime(time).timestamp()))
            else:
                url = url_base
        else:
            url = url_base
    r = requests.get(url)
    price_temp = json.loads(r.content)['result']
    if len(price_temp)==0:
        return
    temp_price.append(price_temp)
    unix_end = int(price_temp[0]['time']/1000)
    while len(price_temp)>1:
        url_chain = url_base + '&end_time=' + str(unix_end)
        r = requests.get(url_chain)
        price_temp = json.loads(r.content)['result']
        temp_price.append(price_temp)
        unix_end = int(price_temp[0]['time']/1000)
    temp_price = list(itertools.chain(*temp_price))
    price_frame = pd.DataFrame(temp_price)
    price_frame.drop_duplicates(inplace=True)
    price_frame['time'] = price_frame['time'].apply(lambda x : int(x*1000000))
    price_frame.set_index('time',inplace=True)
    price_frame.index = pd.to_datetime(price_frame.index)
    price_frame.sort_index(inplace=True)
    return price_frame

def get_quarterly_funding(index_price, ticker):
    active_futures_url = 'https://ftx.com/api/futures'
    r = requests.get(active_futures_url)
    active_futures = json.loads(r.content)['result']
    active_quarter = [future for future in active_futures if future['underlying']==ticker and future['group']=='quarterly' and future['type']=='future'][::-1]

    expired_futures_url = 'https://ftx.com/api/expired_futures'
    r = requests.get(expired_futures_url)
    expired_futures = json.loads(r.content)['result']
    expired_quarterly_futures = [future for future in expired_futures if future['underlying']==ticker and future['group']=='quarterly' and future['type']=='future']
    quarterly_futures = active_quarter+expired_quarterly_futures
    quarterly_list = []
    for quarterly in quarterly_futures[::-1]: 
        test = get_historical_price(quarterly['name'], time = quarterly['expiry'])
        if test is None:
            continue
        test['time_to_expiry'] = pd.to_datetime(quarterly['expiry'])-pd.to_datetime(test['startTime'])
        test['hours_to_expiry'] = test['time_to_expiry'].apply(lambda x: x.total_seconds()//3600)
        test = pd.merge(test, index_price['index'],right_index=True,left_index=True,how='left')
        test['basis'] = (test['close']-test['index'])/test['close']
        test['hourly_funding_rate'] = (test['basis']/test['hours_to_expiry'])
        quarterly_list.append(test['hourly_funding_rate'].rename(quarterly['name']))
        
    quarterly_frame = pd.concat(quarterly_list,axis=1)
    quarterly_frame.dropna(axis=0,how='all',inplace=True)
    quarterly_frame.replace([np.inf, -np.inf], np.nan, inplace=True)
    return quarterly_frame
    
def get_active_quarterly_funding(quarterly_frame):
    quarterly_funding_frame = pd.DataFrame(index = quarterly_frame.index,columns = ['F1'])
    for i,column in enumerate(quarterly_frame.columns): #should be sorted in order
        temp_series = quarterly_frame[column].dropna()
        if i==0: #first column
            quarterly_funding_frame['F1'] = temp_series
        else:
            if pd.isnull(quarterly_funding_frame['F1']).sum()==0:
                continue
            else:
                start_time = quarterly_funding_frame.index[pd.isnull(quarterly_funding_frame['F1'])][0]
                quarterly_funding_frame.loc[quarterly_funding_frame.index >= start_time, 'F1'] = temp_series
    quarterly_funding_frame.index = quarterly_funding_frame.index.tz_localize('UTC')
    return quarterly_funding_frame
                
def get_liq_price(entry_price, quantity, balance, maintenance_margin):
    liq_price = (balance + entry_price*quantity) / (maintenance_margin*quantity + quantity)
    return liq_price
    
def combine_funding_strats(quarterly_dict, funding_dict, ticker,lock_in_rate,rolling_lookback,quantile = .95):
    quarterly_expiry_dates = quarterly_dict[ticker].columns
    quarterly_expiry_dates = pd.to_datetime(['2021'+date.split(ticker+'-')[1] if len(date.split(ticker+'-')[1])==4 else date.split(ticker+'-')[1] for date in quarterly_expiry_dates])
    expiry_stats = []
    funding_full = []
    for i in range(len(quarterly_expiry_dates)-2):
        start_date = quarterly_expiry_dates[i]
        end_date = quarterly_expiry_dates[i+1]
        temp_funding = funding_dict[ticker][start_date:end_date]
        mean_perp = temp_funding['Perp'].mean()*24*365
        high_perp = temp_funding['Perp'].quantile(quantile)*24*365
        mean_future = temp_funding['F1'].mean()*24*365
        high_future = temp_funding['F1'].quantile(quantile)*24*365
        temp_stats = pd.Series([mean_perp,high_perp,mean_future,high_future],index=['mean_perp','high_perp','mean_future','high_future'],name = quarterly_expiry_dates[i])
        expiry_stats.append(temp_stats)
        
        cumsum_perp_funding = temp_funding['Perp'].cumsum()
        signal_perp = (cumsum_perp_funding-cumsum_perp_funding.rolling(rolling_lookback).mean())
        temp_funding['signal'] = (signal_perp<0) & (temp_funding['F1']*24*364>lock_in_rate)
        temp_funding['Combined'] =  temp_funding['Perp'] #default to the perp
        if  temp_funding['signal'].sum() != 0:
            trigger_time = temp_funding['signal'].idxmax()
            temp_funding['Combined'].loc[trigger_time:] =  temp_funding['F1'][trigger_time]
        funding_full.append(temp_funding)

    funding_full = pd.concat(funding_full)
    funding_full = funding_full[~funding_full.index.duplicated(keep='last')]
    expiry_stats = pd.concat(expiry_stats,axis=1).T
    return funding_full, expiry_stats

def get_hedging_costs(perp, perp_funding, port_size=10000, fee = .02*.01):
    amount = port_size/perp['close'][0]
    funding = (amount*perp['close'].resample('1h').last()*perp_funding['rate']).sum()
    entry_fee = (port_size*fee)*-1
    exit_fee = (amount*perp['close'][-1]*fee)*-1
    total_cost = (entry_fee + exit_fee + funding).round(3)
    return total_cost

if __name__ == '__main__':
	
    tickers = ['BTC','ETH']
    funding_dict = {}
    index_dict = {}
    quarterly_dict = {}
    for ticker in tickers: 
        perp_funding_frame = get_perp_funding(ticker+'-PERP')
        index_price = get_historical_price(ticker, index=True)
        index_price.rename({'close':'index'},axis=1,inplace=True)
        quarterly_frame = get_quarterly_funding(index_price, ticker)
        quarterly_funding_frame = get_active_quarterly_funding(quarterly_frame)
    
        funding_frame = pd.merge(perp_funding_frame[['Perp']],quarterly_funding_frame[['F1']],left_index=True,right_index=True)
        funding_dict[ticker] = funding_frame
        index_dict[ticker] = index_price
        quarterly_dict[ticker] = quarterly_frame
    
    