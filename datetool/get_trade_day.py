#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/19
"""

import time
import pandas as pd
import sqlite3 as lite
import quantlib as lib


########################################################################
class GetTradeDay(lib.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log):
        """Constructor"""
        super(GetTradeDay, self).__init__(dbinfocfgpath, log)
        
        msg = "Initial class 'GetTradeDay'"
        self.log.info(msg)
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        self.conn = lite.connect(db_address)
        self.conn.text_factory = str
        
        self.trade_days_dict = {}
        
        cur = self.conn.cursor()
        sql = "select Date from market_data_trade_day order by Date"
        cur.execute(sql)
        rows = cur.fetchall()
        date = []
        for row in rows:
            date.append(row[0])
        self.trade_days = pd.DataFrame(date, index=date)           
               
            
    #----------------------------------------------------------------------
    def find_trade_day_sql(self, day, offset):
        """"""
        tm1 = time.time()
        _key = day+str(offset)
        if self.trade_days_dict.has_key(_key):
            _val = self.trade_days_dict[_key]
        else:
            cur = self.conn.cursor()
            if offset>0:
                sql = """
                      select Date 
                      from market_data_trade_day
                      where date>='{}'
                      order by date asc
                      limit {}
                      """.format(day, abs(offset))
            else:
                sql = """
                      select Date 
                      from market_data_trade_day
                      where date<'{}'
                      order by date desc
                      limit {}
                      """.format(day, abs(offset))   
            cur.execute(sql)
            rows = cur.fetchall()
            _val = rows[-1][0]
            self.trade_days_dict[_key] = _val
        tm2 = time.time()
        #print tm2-tm1
        return _val
    
    
    #----------------------------------------------------------------------
    def find_trade_day_panda(self, date, offset):
        """"""
        tm1 = time.time()
        if offset>0:
            _val = self.trade_days[self.trade_days.index>=date].index[offset-1]
        else:
            _val = self.trade_days[self.trade_days.index<date].index[-offset]
        tm2 = time.time()
        #print tm2-tm1
        return _val
    
    
    #----------------------------------------------------------------------
    def check_stock_trade_status(self, stkcode, check_date, offset):
        """"""
        cur = self.conn.cursor()
        sql = """
              select Status 
              from market_data_a_share
              where StkCode='{}' and Date>='{}'
              order by date asc
              limit {}
              """.format(stkcode, check_date, abs(offset))   
        cur.execute(sql)
        _val = cur.fetchone()[0]
        return _val
    
    
    #----------------------------------------------------------------------
    def get_latest_close_price(self, stkcode, check_date, *arg):
        """"""
        cur = self.conn.cursor()
        sql = """
              select ClosePrice
              from market_data_a_share
              where StkCode='{}' and Date<'{}'
              order by date desc
              limit 1
              """.format(stkcode, check_date)   
        cur.execute(sql)
        _val = cur.fetchone()[0]
        return _val        
            