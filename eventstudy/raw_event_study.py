#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose:
  Created: 2016/7/13
"""

import datetime
import sqlite3 as lite
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
    def find_raw_event(self, event_table, 
                     test_start_date, test_end_date,
                     event_start_mark, event_end_mark, 
                     event_str, constituent_index, sql=None):
        """"""
        msg = "Start to define event"
        self.log.info(msg)
        
        if sql is None:
            sql = """
                  select {}.StkCode,{},ifnull({},{})
                  from {} 
                  left join info_data_index_constituent 
                  on {}.StkCode=info_data_index_constituent.StkCode
                  where IndexCode='{}' and {}
                  and DateInclude<={} and (DateExclude>{} or DateExclude isnull)
                  and {}>='{}' and {}<='{}'
                  """.format(event_table, event_start_mark, event_end_mark, self.today,
                             event_table, 
                             event_table, 
                             constituent_index, event_str,
                             event_start_mark, event_end_mark,
                             event_start_mark, test_start_date, event_start_mark, test_end_date)
        
        self.events = []
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            self.events.append(row)
            
            
    #----------------------------------------------------------------------
    def _calc_event_return(self, event_info, look_ahead_days, look_back_days):
        """"""
        event_day = event_info[1]
        event_end_day = event_info[2]
        test_end_day = min(self.trdday.find_trade_day(event_day, look_ahead_days),
                           event_end_day)
        test_start_day = self.trdday.find_trade_day(event_day, -look_back_day)
        stockcode = [event_info[0]]
        security_type = 'a_stock'
        period = 1
        return_type = 'log'
        
        self.fetchdata.fetch_return()
        
        
        
    #----------------------------------------------------------------------
    def plot_event(self):
        """"""
        msg = "Plot the event"
        self.log.info(msg)
        
        
        
 
        
        
        
        
        
        
    