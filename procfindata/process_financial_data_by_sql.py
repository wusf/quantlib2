#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/21
"""

import sys
import codecs
import sqlite3 as lite
import time
from datetime import datetime
from datetime import timedelta
from multiprocessing.dummy import Pool 
from ConfigParser import ConfigParser
import quantlib as qt
import procfindata.financial_item_algos_by_sql


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
        self.conn = lite.connect(db_address, check_same_thread = False)
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
    def load_local_db_into_memory(self):
        """"""
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        
        self.conn.close()
        self.conn = lite.connect(':memory:', check_same_thread = False)
        self.conn.text_factory = str
        cur = self.conn.cursor()        
        cur.execute("ATTACH '{}' as findata".format(db_address))
        cur.execute("CREATE TABLE financial_data_Balance_Sheet AS "
                    "SELECT * FROM findata.financial_data_Balance_Sheet")
        cur.execute("CREATE TABLE financial_data_Income_Statement AS "
                    "SELECT * FROM findata.financial_data_Income_Statement")
        cur.execute("CREATE TABLE financial_data_CashFlow_Statement AS "
                    "SELECT * FROM findata.financial_data_CashFlow_Statement") 
        cur.execute("DETACH findata")
        
        cur.execute("CREATE INDEX Id1 ON financial_data_Balance_Sheet (StkCode,DATE,ReportingPeriod)")
        cur.execute("CREATE INDEX Id2 ON financial_data_Income_Statement (StkCode,DATE,ReportingPeriod)")
        cur.execute("CREATE INDEX Id3 ON financial_data_CashFlow_Statement (StkCode,DATE,ReportingPeriod)")      
        
        
        
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
    def process(self, thread_pool=10):
        """"""
        msg = "Start to process financial data"
        self.log.info(msg)
        
        cur = self.conn.cursor()

        for stk in self.all_stock_code:
            tm1 = time.time()
            for date in sorted(self.days_when_data_announce[stk].keys()):
                this_fin_qt = self.days_when_data_announce[stk][date][0]
                fin_qts = self._get_past_fin_quarters(this_fin_qt, 10)
                company_type = self.days_when_data_announce[stk][date][1]
                #print stk,date,fin_qts,company_type
                
                result_dict = {}
                #pool = Pool(10)
                #for name in self.fin_item_names:
                #    func = self.fin_item_algos[name].calc
                #    pool.apply_async(func, args=(self.conn, stk, date, 
                #                                fin_qts, result_dict))
                #pool.close()
                #pool.join()
                for name in self.fin_item_names:
                    self.fin_item_algos[name].calc(self.conn, stk, date, 
                                                   fin_qts, result_dict)
            tm2 = time.time()
            print stk,tm2-tm1
                
                
                
                
    #----------------------------------------------------------------------
    def _get_financial_item_algos(self, cfgfilepath):
        """"""
        fin_item_cfg = ConfigParser()
        fin_item_cfg.optionxform = str
        fin_item_cfg.readfp(codecs.open(cfgfilepath, 'r', 'utf-8-sig'))
        self.fin_item_names = fin_item_cfg.options('financial_item')
        self.fin_item_algos = {}
        for name in self.fin_item_names:
            exec("import procfindata.financial_item_algos_by_sql.{} as finalgo".format(name))
            self.fin_item_algos[name] = finalgo
        #self.fin_item_algos[name].calc()
        
        

                
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
                
