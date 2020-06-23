import requests
import json
import quandl
import pandas as pd
import numpy as np
import datetime
import asyncio
import aiomoex
import re

now = datetime.datetime.now()
quandl.ApiConfig.api_key = 'Ms7mH6Hw3wsdt9sc7sz3APfxx' # my key changed for securuty

def strToDate(datestr):
    return datetime.datetime.strptime(datestr, '%Y-%m-%d')

def curr_month_futers(type_dataset, product, start_year):
    ''' формируем список всех будущих запросов по годам и месяцам''' 
    requests = []
    year = np.arange(start_year, now.year+1, 1)
    month = np.array(['F', 'G', 'H', 'J', 'K', 'M', 'N','Q', 'U', 'V', 'X', 'Z'])
    for y in year:
        for m in month:
            s = type_dataset + product + m + str(y)
            requests.append(s)
    return requests


def get_futur_quandl(area, prod, start_year):
    '''формирмируем помесячные датасеты для фьючерса и соединяем их в один'''
    requests = curr_month_futers(area, prod, 2015)
    for i in range(0,len(requests)):
        try:
            if i == 0:
                df = quandl.get(requests[i])
            else:
                raw = quandl.get(requests[i])
                raw = raw.loc[raw.index > max(df.index)]
                df = pd.concat([df, raw])
        except Exception as e:
            print(e, requests[i])     
    name_file = area[:-1] + prod + '.csv'
    df.to_csv(name_file, index=True)
    return df
 
def getDate(time):
    '''  на входе количество микросекунд, возвращает дату в формате datetime'''
    if time > 9999999999:
        time = time / 1000.0
    date = datetime.datetime.fromtimestamp(time) 
    return date.date()

def currency(from_pair,to_pair,limit):
    ''' добываем курсы валют, возвращет датафрейм и сохраняет его в файл, пример currency('USD','RUB',365)'''
    link = 'https://min-api.cryptocompare.com/data/v2/histoday?fsym={}&tsym={}&limit={}'.format(from_pair, to_pair, limit)
    data_str = requests.get(link).text
    data_json = json.loads(data_str)
    candlestick = json.dumps(data_json['Data']['Data'])
    df_tmp = pd.read_json(candlestick)
    df_tmp = df_tmp[['time','close']]
    df_tmp['date'] = df_tmp['time'].apply(getDate)
    df_tmp = df_tmp[['date','close']]
    df_tmp['date'] = pd.to_datetime(df_tmp['date'])
    df_tmp = df_tmp.set_index('date')
    name_file = from_pair + to_pair + '.csv'
    df_tmp.to_csv(name_file, index=True)
    return df_tmp

def get_spot_quandl(tecker, start_date):
    '''  на входе имя инструмента и дата начала, на выходе датафрейм, и сохраняет его в файл'''
    df_tmp = quandl.get(tecker, start_date=start_date)
    name_file = re.sub("/", "", tecker) + '.csv'
    df_tmp.to_csv(name_file, index=True)
    return df_tmp