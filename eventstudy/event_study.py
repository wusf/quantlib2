#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose:
  Created: 2016/7/13
"""

import datetime
import sqlite3 as lite
import pandas as pd
import matplotlib.pyplot as plt
from pylab import *  
mpl.rcParams['font.sans-serif'] = ['SimHei'] 
mpl.rcParams['axes.unicode_minus'] = False
import quantlib as qt
import investuniversetool.set_stock_universe as stkuniver
import datetool.get_trade_day as gtrdday
import fetchdatatool.fetch_hist_eqty_data as fetchdata


########################################################################
class EventStudy(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log):
        """Constructor"""
        super(EventStudy, self).__init__(dbinfocfgpath, log)
        
        msg = "Initiate class EventStudy"
        self.log.info(msg)
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        self.conn = lite.connect(db_address)
        self.conn.text_factory = str        
        
        self.today = datetime.date.today().strftime('%Y%m%d')
        self.start_date = ''
        self.end_date = ''
        
        self.events = []
        
        self.trdday = gtrdday.GetTradeDay(dbinfocfgpath, log)
        
        self.fetchdata = fetchdata.FetchHistData(dbinfocfgpath, log)
        
        
    #----------------------------------------------------------------------
    def find_raw_event(self, event_table, event_special_variable,
                     test_start_date, test_end_date,
                     event_start_mark, event_end_mark, 
                     event_str, constituent_index, sql=None):
        """"""
        msg = "Start to define event"
        self.log.info(msg)
        
        if sql is None:
            sql = """
                  select {}.StkCode,{},ifnull({},'{}'),{}
                  from {} 
                  left join info_data_index_constituent 
                  on {}.StkCode=info_data_index_constituent.StkCode
                  where IndexCode in {} and {}
                  and DateInclude<={} and (DateExclude>{} or DateExclude isnull)
                  and {}>='{}' and {}<='{}'
                  """.format(event_table, event_start_mark, event_end_mark, self.today, event_special_variable,
                             event_table, 
                             event_table, 
                             tuple(constituent_index), event_str,
                             event_start_mark, event_end_mark,
                             event_start_mark, test_start_date, event_start_mark, test_end_date)
        
        self.events = []
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            self.events.append(row)
            
                        
    #----------------------------------------------------------------------
    def _calc_event_return(self, event_info, look_back_days, look_ahead_days, 
                           is_hedged, hedged_index):
        """"""
        event_day = event_info[1]
        event_end_day = event_info[2]
        test_end_day = min(self.trdday.find_trade_day_sql(event_day, look_ahead_days),
                           event_end_day)
        test_start_day = self.trdday.find_trade_day_sql(event_day, -(look_back_days+1))
        stockcode = [event_info[0]]
        security_type = 'a_stock'
        period = 1
        return_type = 'simple'
        
        if is_hedged == 1:
            ret = self.fetchdata.fetch_hedged_return(stockcode, security_type,
                                               test_start_day, test_end_day, period,
                                               return_type, hedged_index)
        else:
            ret = self.fetchdata.fetch_return(stockcode, security_type,
                                               test_start_day, test_end_day, period,
                                               return_type)
        new_index = range(-(look_back_days-1),-(look_back_days-1)+len(ret.index))
        date_df = pd.DataFrame(ret.index, index=new_index, columns=stockcode)
        ret_df = pd.DataFrame(ret.values, index=new_index, columns=stockcode)
        return ret_df,date_df
          
          
    #----------------------------------------------------------------------
    def _event_filter(self, event):
        """"""
        stockcode = event[0]
        event_day = event[1]
        _val = self.trdday.check_stock_trade_status(stockcode, event_day, 1)        
        return -_val
    
        
    #----------------------------------------------------------------------
    def plot_event(self, event_name,
                   look_back_days, look_ahead_days, is_hedged, hedged_index):
        """"""
        msg = "Plot the event"
        self.log.info(msg)
        
        event_ret = pd.DataFrame()
        for event in self.events:
            _filter = self._event_filter(event)
            if _filter == 1:
                msg = "calculate event return-{}".format(event)
                self.log.info(msg)   
                
                ret = self._calc_event_return(event, look_back_days, 
                                              look_ahead_days, 
                                              is_hedged, 
                                              hedged_index)
                event_ret = pd.concat([event_ret,ret[0]],axis=1)
            else:
                msg = "Event-{} is not qualified".format(event)
                self.log.info(msg)                 
                
        event_cum_ret = event_ret.cumsum()-event_ret.cumsum().loc[0]
        ret = event_cum_ret.mean(axis=1)
        std = event_cum_ret.std(axis=1)
        
        plt.plot(ret.index, ret.values)
        plt.axvline(x=0, color='black')
        plt.axhline(y=0, color='black')
        #plt.xticks(range(ret.index[5],ret.index[-5]+5,5))
        plt.xlabel('days')
        plt.ylabel('excess return')
        plt.grid(which='both', linestyle='--')
        plt.title(event_name)
        plt.show()
        return ret,std
        
 
                
        
        
        
        
        
        
        
 
        
        
        
        
        
        
    