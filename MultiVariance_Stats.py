# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 11:37:20 2017

@author: bjwangwenhui
"""

import os
import sys
import math
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.tseries.offsets import Day, MonthEnd, MonthBegin
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn

'''
表：total_iomoney_ionum
字段：realdate、inorout、	inoutmoney、	io_num		
获得
1.total_inmoney & 2.total_in_num & 3.total_outmoney & 4. total_out_num &5.total_net_inmoney
'''
df = pd.read_csv('total_iomoney_ionum.csv')
df.realdate = pd.to_datetime(df.realdate)
total_inmoney = df[df['inorout'] == 'A'].drop('inorout', axis= 1).groupby('realdate')['inoutmoney'].sum().reset_index().set_index('realdate')
total_in_num = df[df['inorout'] == 'A'].drop('inorout', axis= 1).groupby('realdate')['io_num'].sum().reset_index().set_index('realdate')
total_outmoney = df[df['inorout'] == 'B'].drop('inorout', axis= 1).groupby('realdate')['inoutmoney'].sum().reset_index().set_index('realdate')
total_out_num = df[df['inorout'] == 'B'].drop('inorout', axis= 1).groupby('realdate')['io_num'].sum().reset_index().set_index('realdate')
total_net_inmoney = total_inmoney.sub(total_outmoney, fill_value=0) #***



'''
表：total_billsec_worksec_telnum
字段：realdate、	total_billsec、 	total_worksec、	total_tel_num
获得
6.total_billsec & 7.total_worksec & 8.total_tel_num
'''
df = pd.read_csv('total_billsec_worksec_telnum.csv')
df.realdate = pd.to_datetime(df.realdate)
total_billsec = df[['realdate', 'billsec']].set_index('realdate')
total_worksec = df[['realdate', 'worksec']].set_index('realdate')
total_tel_num = df[['realdate', 'tel_num']].set_index('realdate')
#%%
'''
通过字段total_net_inmoney和total_billsec
获得
9.total_net_inmoney_per_min
'''
total_net_inmoney_per_min = total_net_inmoney['inoutmoney'].div(total_billsec['billsec'] / 60.0)  #**
#%%
'''
表hl
字段：user_id, firm_id,hl
right JOIN
表：transfer
字段：user_id, firm_id,realdate,inorout,inoutmoney,io_num
ON user_id
获得
high_inmoney、high_in_num 、high_outmoney 、 high_out_num 、 high_net_inmoney
common_inmoney、common_in_num 、common_outmoney 、common_out_num 、common_net_inmoney
'''
transfer = pd.read_csv('transfer.csv')
transfer.realdate = pd.to_datetime(transfer.realdate)
hl = pd.read_csv('hl.csv')
hl['hl'][hl['hl'] == -1] = 0

df = pd.merge(hl, transfer, on= 'user_id', how= 'left')

## high_inmoney、high_outmoney
high =df[['user_id', 'realdate', 'inorout', 'inoutmoney', 'io_num']][df['hl'] == 1].groupby(['realdate', 'inorout'])['inoutmoney'].sum().reset_index().set_index('realdate')
high_inmoney = high[high.inorout == 'A'].drop('inorout', axis= 1)
high_outmoney = high[high.inorout == 'B'].drop('inorout', axis= 1)

## common_inmoney、common_outmoney
common =df[['user_id', 'realdate', 'inorout', 'inoutmoney', 'io_num']][df['hl'] == 0].groupby(['realdate', 'inorout'])['inoutmoney'].sum().reset_index().set_index('realdate')
common_inmoney = common[common.inorout == 'A'].drop('inorout', axis= 1)
common_outmoney = common[common.inorout == 'B'].drop('inorout', axis= 1)

## high_in_num 、high_out_num 
high =df[['user_id', 'realdate', 'inorout', 'inoutmoney', 'io_num']][df['hl'] == 1].groupby(['realdate', 'inorout'])['io_num'].sum().reset_index().set_index('realdate')
high_in_num = high[high.inorout == 'A'].drop('inorout', axis= 1)
high_out_num = high[high.inorout == 'B'].drop('inorout', axis= 1)

## common_in_num 、common_out_num 
common =df[['user_id', 'realdate', 'inorout', 'inoutmoney', 'io_num']][df['hl'] == 0].groupby(['realdate', 'inorout'])['io_num'].sum().reset_index().set_index('realdate')
common_in_num = common[common.inorout == 'A'].drop('inorout', axis= 1)
common_out_num = common[common.inorout == 'B'].drop('inorout', axis= 1)

## high_net_inmoney
high_net_inmoney = high_inmoney.sub(high_outmoney, fill_value=0)

## common_net_inmoney
common_net_inmoney = common_inmoney.sub(common_outmoney, fill_value=0)
#%%
'''
表hl
字段：user_id, firm_id,hl
LEFT JOIN
表：tel
字段：user_id、realdate、billsec、bill_num、worksec
ON user_id
获得
high_billsec、high_worksec 、high_tel_num、high_net_inmoney_per_min
common_billsec、common_worksec、common_tel_num、common_net_inmoney_per_min
high_tel_num_ratio、common_tel_num_ratio
high_billsec_ratio、common_billsec_ratio
high_worksec_ratio、common_worksec_ratio
'''
tel = pd.read_csv('tel.csv')
tel.realdate = pd.to_datetime(tel.realdate)
df = pd.merge(hl, tel, on= 'user_id', how= 'left')

## high_billsec、high_worksec、high_tel_num
high_billsec = df[df['hl'] == 1].groupby('realdate')['billsec'].sum().reset_index().set_index('realdate')
high_worksec = df[df['hl'] == 1].groupby('realdate')['worksec'].sum().reset_index().set_index('realdate')
high_tel_num = df[df['hl'] == 1].groupby('realdate')['bill_num'].sum().reset_index().set_index('realdate')

## common_billsec、common_worksec、common_tel_num
common_billsec = df[df['hl'] == 0].groupby('realdate')['billsec'].sum().reset_index().set_index('realdate')
common_worksec = df[df['hl'] == 0].groupby('realdate')['worksec'].sum().reset_index().set_index('realdate')
common_tel_num = df[df['hl'] == 0].groupby('realdate')['bill_num'].sum().reset_index().set_index('realdate')

## high_net_inmoney_per_min
#df = pd.merge(high_net_inmoney.reset_index()  , high_billsec.reset_index(), on= 'realdate', how= 'left').set_index('realdate')
high_inmoney_per_min = high_inmoney['inoutmoney'].div(high_billsec['billsec'] / 60.0) #***
#high_net_inmoney_per_min = (high_net_inmoney['net_inmoney'].astype(np.float) / (high_billsec['billsec'].astype(np.float) / 60.0)).reset_index().set_index('realdate')
high_net_inmoney_per_min = high_net_inmoney['inoutmoney'].div(high_billsec['billsec'] / 60.0) #***

## common_net_inmoney_per_min
#df = pd.merge(common_net_inmoney.reset_index(), common_billsec.reset_index(), on= 'realdate', how= 'left').set_index('realdate')
#common_net_inmoney_per_min = (common_net_inmoney['net_inmoney'].astype(np.float) / (common_billsec['billsec'].astype(np.float) / 60.0)).reset_index().set_index('realdate')
common_inmoney_per_min = common_inmoney['inoutmoney'].div(common_billsec['billsec'] / 60.0)
common_net_inmoney_per_min = common_net_inmoney['inoutmoney'].div(common_billsec['billsec'] / 60.0)

## high_tel_num_ratio
high_tel_num_ratio = (high_tel_num['bill_num'].astype(np.float) / (high_tel_num['bill_num'].astype(np.float) + common_tel_num['bill_num'].astype(np.float))).reset_index().set_index('realdate')

## common_tel_num_ratio
common_tel_num_ratio = (common_tel_num['bill_num'].astype(np.float) / (high_tel_num['bill_num'].astype(np.float) + common_tel_num['bill_num'].astype(np.float))).reset_index().set_index('realdate')


## high_billsec_ratio
high_billsec_ratio = (high_billsec['billsec'].astype(np.float) / (high_billsec['billsec'].astype(np.float) + common_billsec['billsec'].astype(np.float))).reset_index().set_index('realdate')

## common_billsec_ratio
common_billsec_ratio = (common_billsec['billsec'].astype(np.float) / (high_billsec['billsec'].astype(np.float) + common_billsec['billsec'].astype(np.float))).reset_index().set_index('realdate')

## high_worksec_ratio
high_worksec_ratio = (high_worksec['worksec'].astype(np.float) / (high_worksec['worksec'].astype(np.float) + common_worksec['worksec'].astype(np.float))).reset_index().set_index('realdate')

## common_worksec_ratio
common_worksec_ratio = (common_worksec['worksec'].astype(np.float) / (high_worksec['worksec'].astype(np.float) + common_worksec['worksec'].astype(np.float))).reset_index().set_index('realdate')


# %%
'''
将上述33个字段连接成一张表，dailyStatist
'''
dailyStatist = pd.concat([total_inmoney, total_in_num, total_outmoney, total_out_num, \
                       total_net_inmoney, total_billsec, total_worksec, total_tel_num, \
                       total_net_inmoney_per_min, \
                       high_inmoney, high_in_num, high_outmoney, high_out_num, high_net_inmoney, \
                       high_billsec, high_worksec, high_tel_num, high_inmoney_per_min, high_net_inmoney_per_min, \
                       high_tel_num_ratio, high_billsec_ratio, high_worksec_ratio, \
                       common_inmoney, common_in_num, common_outmoney, common_out_num, common_net_inmoney, \
                       common_billsec, common_worksec,common_tel_num, common_inmoney_per_min, common_net_inmoney_per_min, \
                       common_tel_num_ratio, common_billsec_ratio, common_worksec_ratio], axis= 1).astype(str, inplace= True)
keys= ['total_inmoney', 'total_in_num', 'total_outmoney', 'total_out_num', \
                       'total_net_inmoney', 'total_billsec', 'total_worksec', 'total_tel_num', \
                       'total_net_inmoney_per_min', \
                       'high_inmoney', 'high_in_num', 'high_outmoney', 'high_out_num', 'high_net_inmoney', \
                       'high_billsec', 'high_worksec', 'high_tel_num', 'high_inmoney_per_min', 'high_net_inmoney_per_min', \
                       'high_tel_num_ratio', 'high_billsec_ratio', 'high_worksec_ratio', \
                       'common_inmoney', 'common_in_num', 'common_outmoney', 'common_out_num', 'common_net_inmoney', \
                       'common_billsec', 'common_worksec', 'common_tel_num', 'common_inmoney_per_min', 'common_net_inmoney_per_min', \
                       'common_tel_num_ratio', 'common_billsec_ratio', 'common_worksec_ratio']
keys1 = ['总入金', '总入金人次', '总出金', '总出金人次', '总净入金', '总billsec', '总worksec', '总拨打电话数', '每分钟总净入金', \
        '高价值入金', '高价值入金人次', '高价值出金', '高价值出金人次', '高价值净入金', \
        '高价值billsec', '高价值worksec', '高价值拨打电话数', '每分钟高价值入金', '每分钟高价值净入金', \
        '高价值拨打电话数占比', '高价值billsec占比', '高价值worksec占比', \
        '普通价值入金', '普通价值入金人次', '普通价值出金', '普通价值出金人次', '普通价值净入金', \
        '普通价值billsec', '普通价值worksec', '普通价值拨打电话数', '每分钟普通价值入金', '每分钟普通价值净入金', \
        '普通价值拨打电话数占比', '普通价值billsec占比', '普通价值worksec占比']
        
             
dailyStatist.columns = keys
dailyStatist.sort_index(ascending=False, inplace=True)
dailyStatist = dailyStatist.reset_index()
dailyStatist.to_csv('dailyStatist.csv', encoding="gb2312")

#%%
'''
按周进行重采样，以求和的形式，半开半闭区间
'''
#total
week_total_inmoney = DataFrame(total_inmoney.values, index= pd.DatetimeIndex(total_inmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_total_in_num = DataFrame(total_in_num.values, index= pd.DatetimeIndex(total_in_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_total_outmoney = DataFrame(total_outmoney.values, index= pd.DatetimeIndex(total_outmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                            
week_total_out_num = DataFrame(total_out_num.values, index= pd.DatetimeIndex(total_out_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                           
week_total_net_inmoney = DataFrame(total_net_inmoney.values, index= pd.DatetimeIndex(total_net_inmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                            
week_total_billsec = DataFrame(total_billsec.values, index= pd.DatetimeIndex(total_billsec.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_total_worksec = DataFrame(total_worksec.values, index= pd.DatetimeIndex(total_worksec.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_total_tel_num = DataFrame(total_tel_num.values, index= pd.DatetimeIndex(total_tel_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                          
week_total_net_inmoney_per_min = week_total_net_inmoney.div(week_total_billsec)    
# high            
week_high_inmoney = DataFrame(high_inmoney.values, index= pd.DatetimeIndex(high_inmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_high_in_num = DataFrame(high_in_num.values, index= pd.DatetimeIndex(high_in_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_high_outmoney = DataFrame(high_outmoney.values, index= pd.DatetimeIndex(high_outmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                            
week_high_out_num = DataFrame(high_out_num.values, index= pd.DatetimeIndex(high_out_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                           
week_high_net_inmoney = DataFrame(high_net_inmoney.values, index= pd.DatetimeIndex(high_net_inmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                            
week_high_billsec = DataFrame(high_billsec.values, index= pd.DatetimeIndex(high_billsec.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_high_worksec = DataFrame(high_worksec.values, index= pd.DatetimeIndex(high_worksec.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_high_tel_num = DataFrame(high_tel_num.values, index= pd.DatetimeIndex(high_tel_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                          
week_high_inmoney_per_min = week_high_inmoney.div(week_high_billsec / 60.0)
week_high_net_inmoney_per_min = week_high_net_inmoney.div(week_high_billsec / 60.0)      
# common
week_common_inmoney = DataFrame(common_inmoney.values, index= pd.DatetimeIndex(common_inmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_common_in_num = DataFrame(common_in_num.values, index= pd.DatetimeIndex(common_in_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_common_outmoney = DataFrame(common_outmoney.values, index= pd.DatetimeIndex(common_outmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                            
week_common_out_num = DataFrame(common_out_num.values, index= pd.DatetimeIndex(common_out_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                           
week_common_net_inmoney = DataFrame(common_net_inmoney.values, index= pd.DatetimeIndex(common_net_inmoney.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                            
week_common_billsec = DataFrame(common_billsec.values, index= pd.DatetimeIndex(common_billsec.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_common_worksec = DataFrame(common_worksec.values, index= pd.DatetimeIndex(common_worksec.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')
week_common_tel_num = DataFrame(common_tel_num.values, index= pd.DatetimeIndex(common_tel_num.index)) \
                            .resample('W-MON', how= 'sum', closed= 'left', label= 'right')                          
week_common_inmoney_per_min = week_common_inmoney.div(week_common_billsec / 60.0)                            
week_common_net_inmoney_per_min = week_common_net_inmoney.div(week_common_billsec / 60.0)  
## high_tel_num_ratio
week_high_tel_num_ratio = (week_high_tel_num[0].astype(np.float) / (week_high_tel_num[0].astype(np.float) + week_common_tel_num[0].astype(np.float))).reset_index().set_index('realdate')
## common_tel_num_ratio
week_common_tel_num_ratio = (week_common_tel_num[0].astype(np.float) / (week_high_tel_num[0].astype(np.float) + week_common_tel_num[0].astype(np.float))).reset_index().set_index('realdate')

## high_billsec_ratio
week_high_billsec_ratio = (week_high_billsec[0].astype(np.float) / (week_high_billsec[0].astype(np.float) + week_common_billsec[0].astype(np.float))).reset_index().set_index('realdate')

## common_billsec_ratio
week_common_billsec_ratio = (week_common_billsec[0].astype(np.float) / (week_high_billsec[0].astype(np.float) + week_common_billsec[0].astype(np.float))).reset_index().set_index('realdate')

## high_worksec_ratio
week_high_worksec_ratio = (week_high_worksec[0].astype(np.float) / (week_high_worksec[0].astype(np.float) + week_common_worksec[0].astype(np.float))).reset_index().set_index('realdate')

## common_worksec_ratio
week_common_worksec_ratio = (week_common_worksec[0].astype(np.float) / (week_high_worksec[0].astype(np.float) + week_common_worksec[0].astype(np.float))).reset_index().set_index('realdate')

weekStatist = pd.concat([week_total_inmoney, week_total_in_num, week_total_outmoney, week_total_out_num, \
                       week_total_net_inmoney, week_total_billsec, week_total_worksec, week_total_tel_num, \
                       week_total_net_inmoney_per_min, \
                       week_high_inmoney, week_high_in_num, week_high_outmoney, week_high_out_num, week_high_net_inmoney, \
                       week_high_billsec, week_high_worksec, week_high_tel_num, week_high_inmoney_per_min, week_high_net_inmoney_per_min, \
                       week_high_tel_num_ratio, week_high_billsec_ratio, week_high_worksec_ratio, \
                       week_common_inmoney, week_common_in_num, week_common_outmoney, week_common_out_num, week_common_net_inmoney, \
                       week_common_billsec, week_common_worksec,week_common_tel_num,  week_common_inmoney_per_min, week_common_net_inmoney_per_min, \
                       week_common_tel_num_ratio, week_common_billsec_ratio, week_common_worksec_ratio], axis= 1).astype(str, inplace= True)


        
             
weekStatist.columns = keys
weekStatist.sort_index(ascending=False, inplace=True)
weekStatist = weekStatist.reset_index()
x = (weekStatist.loc[0, 'realdate'] - 7 *Day()).strftime('%Y-%m-%d') + '--' + dailyStatist.loc[0, 'realdate'].strftime('%Y-%m-%d')
y = weekStatist.loc[1:len(weekStatist) - 2, 'realdate'].apply(lambda x: ((x - 7 * Day()).strftime('%Y-%m-%d')) + '--' + ((x - Day()).strftime('%Y-%m-%d')))
z = dailyStatist.loc[len(dailyStatist) - 1, 'realdate'].strftime('%Y-%m-%d') + '--' + (weekStatist.loc[len(weekStatist) - 1, 'realdate']- Day()).strftime('%Y-%m-%d')
weekStatist['realdate'] = [x] + y.tolist() + [z]
weekStatist.to_csv('weekStatist.csv', encoding="gb2312")

#%%
'''
按月进行重采样，采用MS的形式（每月第一个日历日），以求和的形式，半开半闭区间
'''
#total
mon_total_inmoney = DataFrame(total_inmoney.values, index= pd.DatetimeIndex(total_inmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_total_in_num = DataFrame(total_in_num.values, index= pd.DatetimeIndex(total_in_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_total_outmoney = DataFrame(total_outmoney.values, index= pd.DatetimeIndex(total_outmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                            
mon_total_out_num = DataFrame(total_out_num.values, index= pd.DatetimeIndex(total_out_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                           
mon_total_net_inmoney = DataFrame(total_net_inmoney.values, index= pd.DatetimeIndex(total_net_inmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                            
mon_total_billsec = DataFrame(total_billsec.values, index= pd.DatetimeIndex(total_billsec.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_total_worksec = DataFrame(total_worksec.values, index= pd.DatetimeIndex(total_worksec.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_total_tel_num = DataFrame(total_tel_num.values, index= pd.DatetimeIndex(total_tel_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                          
mon_total_net_inmoney_per_min = mon_total_net_inmoney.div(mon_total_billsec)    
# high            
mon_high_inmoney = DataFrame(high_inmoney.values, index= pd.DatetimeIndex(high_inmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_high_in_num = DataFrame(high_in_num.values, index= pd.DatetimeIndex(high_in_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_high_outmoney = DataFrame(high_outmoney.values, index= pd.DatetimeIndex(high_outmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                            
mon_high_out_num = DataFrame(high_out_num.values, index= pd.DatetimeIndex(high_out_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                           
mon_high_net_inmoney = DataFrame(high_net_inmoney.values, index= pd.DatetimeIndex(high_net_inmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                            
mon_high_billsec = DataFrame(high_billsec.values, index= pd.DatetimeIndex(high_billsec.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_high_worksec = DataFrame(high_worksec.values, index= pd.DatetimeIndex(high_worksec.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_high_tel_num = DataFrame(high_tel_num.values, index= pd.DatetimeIndex(high_tel_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                          
mon_high_inmoney_per_min = mon_high_inmoney.div(mon_high_billsec / 60.0)     
mon_high_net_inmoney_per_min = mon_high_net_inmoney.div(mon_high_billsec / 60.0)      
# common
mon_common_inmoney = DataFrame(common_inmoney.values, index= pd.DatetimeIndex(common_inmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_common_in_num = DataFrame(common_in_num.values, index= pd.DatetimeIndex(common_in_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_common_outmoney = DataFrame(common_outmoney.values, index= pd.DatetimeIndex(common_outmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                            
mon_common_out_num = DataFrame(common_out_num.values, index= pd.DatetimeIndex(common_out_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                           
mon_common_net_inmoney = DataFrame(common_net_inmoney.values, index= pd.DatetimeIndex(common_net_inmoney.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')                            
mon_common_billsec = DataFrame(common_billsec.values, index= pd.DatetimeIndex(common_billsec.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_common_worksec = DataFrame(common_worksec.values, index= pd.DatetimeIndex(common_worksec.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')
mon_common_tel_num = DataFrame(common_tel_num.values, index= pd.DatetimeIndex(common_tel_num.index)) \
                            .resample('M', how= 'sum', closed= 'left', label= 'right')  
mon_common_inmoney_per_min = mon_common_inmoney.div(mon_common_billsec / 60.0)                               
mon_common_net_inmoney_per_min = mon_common_net_inmoney.div(mon_common_billsec / 60.0)  
## high_tel_num_ratio
mon_high_tel_num_ratio = (mon_high_tel_num[0].astype(np.float) / (mon_high_tel_num[0].astype(np.float) + mon_common_tel_num[0].astype(np.float))).reset_index().set_index('realdate')
## common_tel_num_ratio
mon_common_tel_num_ratio = (mon_common_tel_num[0].astype(np.float) / (mon_high_tel_num[0].astype(np.float) + mon_common_tel_num[0].astype(np.float))).reset_index().set_index('realdate')

## high_billsec_ratio
mon_high_billsec_ratio = (mon_high_billsec[0].astype(np.float) / (mon_high_billsec[0].astype(np.float) + mon_common_billsec[0].astype(np.float))).reset_index().set_index('realdate')

## common_billsec_ratio
mon_common_billsec_ratio = (mon_common_billsec[0].astype(np.float) / (mon_high_billsec[0].astype(np.float) + mon_common_billsec[0].astype(np.float))).reset_index().set_index('realdate')

## high_worksec_ratio
mon_high_worksec_ratio = (mon_high_worksec[0].astype(np.float) / (mon_high_worksec[0].astype(np.float) + mon_common_worksec[0].astype(np.float))).reset_index().set_index('realdate')

## common_worksec_ratio
mon_common_worksec_ratio = (mon_common_worksec[0].astype(np.float) / (mon_high_worksec[0].astype(np.float) + mon_common_worksec[0].astype(np.float))).reset_index().set_index('realdate')

monStatist = pd.concat([mon_total_inmoney, mon_total_in_num, mon_total_outmoney, mon_total_out_num, \
                       mon_total_net_inmoney, mon_total_billsec, mon_total_worksec, mon_total_tel_num, \
                       mon_total_net_inmoney_per_min, \
                       mon_high_inmoney, mon_high_in_num, mon_high_outmoney, mon_high_out_num, mon_high_net_inmoney, \
                       mon_high_billsec, mon_high_worksec, mon_high_tel_num, mon_high_inmoney_per_min, mon_high_net_inmoney_per_min, \
                       mon_high_tel_num_ratio, mon_high_billsec_ratio, mon_high_worksec_ratio, \
                       mon_common_inmoney, mon_common_in_num, mon_common_outmoney, mon_common_out_num, mon_common_net_inmoney, \
                       mon_common_billsec, mon_common_worksec,mon_common_tel_num,  mon_common_inmoney_per_min, mon_common_net_inmoney_per_min, \
                       mon_common_tel_num_ratio, mon_common_billsec_ratio, mon_common_worksec_ratio], axis= 1).astype(str, inplace= True)
keys1= ['mon_total_inmoney', 'mon_total_in_num', 'mon_total_outmoney', 'mon_total_out_num', \
                       'mon_total_net_inmoney', 'mon_total_billsec', 'mon_total_worksec', 'mon_total_tel_num', \
                       'mon_total_net_inmoney_per_min', \
                       'mon_high_inmoney', 'mon_high_in_num', 'mon_high_outmoney', 'mon_high_out_num', 'mon_high_net_inmoney', \
                       'mon_high_billsec', 'mon_high_worksec', 'mon_high_tel_num', 'mon_high_net_inmoney_per_min', \
                       'mon_high_tel_num_ratio', 'mon_high_billsec_ratio', 'mon_high_worksec_ratio', \
                       'mon_common_inmoney', 'mon_common_in_num', 'mon_common_outmoney', 'mon_common_out_num', 'mon_common_net_inmoney', \
                       'mon_common_billsec', 'mon_common_worksec', 'mon_common_tel_num', 'mon_common_net_inmoney_per_min', \
                       'mon_common_tel_num_ratio', 'mon_common_billsec_ratio', 'mon_common_worksec_ratio']
 
        
             
monStatist.columns = keys
monStatist.sort_index(ascending=False, inplace=True)
monStatist = monStatist.reset_index()
#x =  (monStatist.loc[0, 'realdate'] - MonthBegin()).strftime('%Y-%m-%d') + '--' + dailyStatist.loc[0, 'realdate'].strftime('%Y-%m-%d')
#y = monStatist.loc[1: len(monStatist) - 2, 'realdate'].apply(lambda x: (x - MonthBegin()).strftime('%Y-%m-%d') + '--' + x.strftime('%Y-%m-%d'))
#z = dailyStatist.loc[len(dailyStatist) - 1, 'realdate'].strftime('%Y-%m-%d') + '--' + monStatist.loc[len(monStatist) - 1, 'realdate'].strftime('%Y-%m-%d')
#monStatist['realdate'] = [x] + y.tolist() + [z]
monStatist.to_csv('monStatist.csv', encoding="gb2312")        
#%%
