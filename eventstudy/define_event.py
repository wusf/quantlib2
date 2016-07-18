#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose:
  Created: 2016/7/13
"""

import quantlib as qt
import investuniversetool.set_stock_universe as stkuniver

########################################################################
class EventStudy(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log):
        """Constructor"""
        super(DefineEvent, self).__init__(dbinfocfgpath, log)
        
        msg = "Initiate class EventStudy"
        self.log.info(msg)
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        self.conn = lite.connect(db_address)
        self.conn.text_factory = str        
        
        self.start_date = ''
        self.end_date = ''
        
        self.stock_universe = stkuniver.SetStockUniverse(dbinfocfgpath, log)
        
        
    #----------------------------------------------------------------------
    def define_event(self, event_data_table, 
                     event_start_date, event_end_date,
                     event_str):
        """"""
        msg = "Start to define event"
        self.log.info(msg)
        
        self.event_data_table = event_data_table
        self.event_date = event_date 
        self.event_threshold = event_str
        
        
    #----------------------------------------------------------------------
    def find_event(self, test_start_date, test_end_date, universe_index):
        """"""
        msg = ("Find all defined events from '{}' to '{}'"
               .format(test_start_date, test_end_date))
        self.log.info(msg)
        
        sql = """
              select {} from {} 
              where {}>='{}' and {}<='{}'
              order by {} asc
              """.format(self.event_date,
                         self.event_data_table,
                         self.event_date,
                         test_start_date,
                         self.event_date,
                         test_end_date)
        date = []
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            date.append(row[0])
            
        for dt in date:
            print dt
        
        
        
        
        
        
    