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
        
        
    #----------------------------------------------------------------------
    def get_stocks_by_index(self, indexcode, date):
        """"""
        if len(self.constituent_adjust_date_list) == 0:
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
            self.constituent_adjust_dates = adjust_date_pair
            self.constituent_adjust_date_position = p
            
        print self.constituent_adjust_dates
                

