#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/15
"""

import sqlite3 as lite
import quantlib as lib


########################################################################
class SetStockUniverse(lib.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbcfgpath, log):
        """Constructor"""
        super(SetStockUniverse, self).__init__(dbcfgpath, log)
        msg = "Initiate class 'SetStockUniverse'"
        self.log.info(msg)
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        self.conn = lite.connect(db_address)
        self.conn.text_factory = str
        
        self.constituent_adjust_dates = ['19900101','1990101']
        self.constituent_adjust_date_list = []
        self.constituent_adjust_date_position = 0
        self.len_constituent_adjust_date = 0
        
        self.constituent_stocks = []
        
        
    #----------------------------------------------------------------------
    def get_stocks_by_index(self, indexcode, date):
        """"""
        if len(self.constituent_adjust_date_list) == 0:
            
            msg = "Get constituent stock of '{}'".format(indexcode)
            self.log.info(msg)
            
            cur = self.conn.cursor()
            sql = """
                  select distinct DateInclude 
                  from info_data_index_constituent
                  where IndexCode='{}'
                  """.format(indexcode)
            cur.execute(sql)
            rows = cur.fetchall()
            i = 0
            for row in rows:
                self.constituent_adjust_date_list.append(row[0])
                i += 1
            self.len_constituent_adjust_date = i
                
        for p in xrange(self.constituent_adjust_date_position,
                        self.len_constituent_adjust_date):
            startdate = self.constituent_adjust_date_list[p]
            enddate = self.constituent_adjust_date_list[p+1]
            if startdate <= date < enddate:
                adjust_date_pair = [startdate, enddate]
                break
                
        if adjust_date_pair != self.constituent_adjust_dates:
            
            msg = ("Constituents of '{}' has been changed, get new stock universe @'{}'".
                   format(indexcode, adjust_date_pair[0]))
            self.log.info(msg)
            
            self.constituent_adjust_dates = adjust_date_pair
            self.constituent_adjust_date_position = p
            
            include_date = adjust_date_pair[0]
            exclude_date = adjust_date_pair[1]
            sql = """
                  select Stkcode 
                  from info_data_index_constituent
                  where DateInclude<='{}'
                  and (DateExclude>'{}' or DateExclude is NULL)
                  and IndexCode = '{}'
                  """.format(include_date, include_date, indexcode)
            cur = self.conn.cursor()
            cur.execute(sql)
            self.constituent_stocks = []
            rows = cur.fetchall()
            for row in rows:
                self.constituent_stocks.append(row[0])
            

                

