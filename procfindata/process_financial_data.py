#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/21
"""

import sys
import sqlite3 as lite
import quantlib as qt


########################################################################
class ProcFinancialData(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, start_date, dbinfocfgpath, log):
        """Constructor"""
        super(ProcFinancialData, self).__init__(dbinfocfgpath, log)
    
        msg = "Initiate class 'ProcFinancialData'"
        self.log.info(msg)    
        
        self.start_date = start_date
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        self.conn = lite.connect(db_address)
        self.conn.text_factory = str     
        
        self.all_stock_code = []
        self.days_when_data_change = {}
        
    
    #----------------------------------------------------------------------
    def _find_all_stock_codes(self):
        """"""
        msg = "Find all stock codes"
        self.log.info(msg)    
        
        cur = self.conn.cursor()
        sql = """
              select distinct StkCode
              from financial_data_balance_sheet
              where Date>='{}'
              """.format(self.start_date)
        cur.execute(sql)
        rows = cur.fetchall()
        self.all_stock_code = []
        for row in rows:
            self.all_stock_code.append(row[0])
            
    
    #----------------------------------------------------------------------
    def _find_days_when_data_change(self):
        """"""
        msg = "Find days when financial data was changed"
        self.log.info(msg)   
        
        cur = self.conn.cursor()
        for stk in self.all_stock_code:
            sql = """
                  select distinct Date 
                  from {}
                  where StkCode='{}' and Date>='{}'
                  order by Date asc
                  """
            date_set = set()
            for _tb in ['balance_sheet','income_statement','cashflow_statement']:
                tb = 'financial_data_'+_tb
                cur.execute(sql.format(tb, stk, self.start_date))
                rows = cur.fetchall()
                _dates = []
                for row in rows:
                    _dates.append(row[0])
                _set = set(_dates)
                date_set = date_set|_set
            date_list = sorted(list(date_set))
            self.days_when_data_change[stk] = date_list
                    
        
         
        
        