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
from datetime import datetime
from datetime import timedelta


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
        self.days_when_data_announce = {}
        
        _year = range(1990,2020)
        year = []
        for y in _year:
            year.append(str(y))
        quarter = ['0331','0630','0930','1231']
        self.financial_quarters = []
        for y in year:
            for q in quarter:
                self.financial_quarters.append(y+q)
        self.past_financial_quarters = {}
        
        
    #----------------------------------------------------------------------
    def find_all_stock_codes(self):
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
    def find_days_when_data_change(self):
        """"""
        msg = "Find days when financial data announced"
        self.log.info(msg)   
        
        date_when_new_announcement = {}
        
        cur = self.conn.cursor()
        sql = """
              select distinct Date 
              from {}
              where StkCode='{}' and Date>='{}'
              order by Date asc
              """        
        for stk in self.all_stock_code:
            date_list = []
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
            date_when_new_announcement[stk] = date_list
        
        sql = """
              select Date,ReportingPeriod,CompanyType
              from {}
              where StkCode='{}' and Date<='{}'
              order by ReportingPeriod desc limit 1
              """
        for stk in self.all_stock_code:
            effective_num_day = 150
            self.days_when_data_announce[stk] = {}
            for date in date_when_new_announcement[stk]:
                rpt_period_list = []
                for _tb in ['balance_sheet','income_statement','cashflow_statement']:
                    tb = 'financial_data_'+_tb                
                    cur.execute(sql.format(tb, stk, date))
                    row = cur.fetchone()
                    if row is not None:
                        _date = datetime.strptime(row[0], '%Y%m%d')
                        _rpt_period = datetime.strptime(row[1], '%Y%m%d')
                        days_diff =  (_date - _rpt_period).days
                        company_type = row[2]
                        if days_diff <= effective_num_day:
                            rpt_period_list.append(row[1])
                if len(rpt_period_list) == 3:
                    if (rpt_period_list[0] == rpt_period_list[1]
                        and rpt_period_list[0] == rpt_period_list[2]):
                        self.days_when_data_announce[stk][date] = [rpt_period_list[0],company_type]
                        
            
    #----------------------------------------------------------------------
    def process(self, multithread):
        """"""
        msg = "Start to process financial data"
        self.log.info(msg)
        
        cur = self.conn.cursor()
        sql = """
              select * from {} where StkCode='{}'
              """
        

        for stk in self.all_stock_code:
            for date in sorted(self.days_when_data_announce[stk].keys()):
                this_fin_qt = self.days_when_data_announce[stk][date][0]
                fin_qts = self._get_past_fin_quarters(this_fin_qt, 10)
                company_type = self.days_when_data_announce[stk][date][1]
                print stk,date,this_fin_qt,company_type
                
                
    #----------------------------------------------------------------------
    def _calc_financial_items(self, multithread):
        """"""
        if multithread == 0:
            print 'multithread'
        esle:
            print 'siglethread'
        

                
    #----------------------------------------------------------------------
    def _get_past_fin_quarters(self, this_fin_quater, n):
        """"""
        if self.past_financial_quarters.has_key(this_fin_quater):
            return self.past_financial_quarters[this_fin_quater]
        else:
            self.past_financial_quarters[this_fin_quater] = []
            p = self.financial_quarters.index(this_fin_quater)
            for i in xrange(n):
                qt = self.financial_quarters[p-i]
                self.past_financial_quarters[this_fin_quater].append(qt)
            return self.past_financial_quarters[this_fin_quater]
                
