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
    def _find_event_return(self):
        """"""
        
    #----------------------------------------------------------------------
    def plot_event(self):
        """"""
        msg = "Plot the event"
        self.log.info(msg)
        
        
        
 
        
        
        
        
        
        
    