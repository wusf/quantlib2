#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/28
"""

import sys
import codecs
import sqlite3 as lite
import time
from datetime import datetime
from datetime import timedelta
import concurrent.futures
from ConfigParser import ConfigParser
import quantlib as qt
import procfindata.financial_item_algos_by_pandas
import fetchdatatool.load_data_into_pandas as loaddb


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
        table_name_list = ['financial_data_balance_sheet',
                           'financial_data_income_statement',
                           'financial_data_cashflow_statement']
        index_name_str = 'StkCode,Date,ReportingPeriod'
        date_col_name = 'Date'
        date = '20070101'
    
        self.table_df = loaddb.load_data_into_pandas(self.conn,
                                                    table_name_list,
                                                    date_col_name, date)

    #----------------------------------------------------------------------
    def find_all_stock_codes(self):
        """"""
        msg = "Find all stock codes"
        self.log.info(msg)    
        tb = self.table_df['financial_data_balance_sheet']
        self.all_stock_code = tb.index.get_level_values('StkCode').unique()


    #----------------------------------------------------------------------
    def find_days_when_data_change(self):
        """"""
        msg = "Find days when financial data announced"
        self.log.info(msg)   
        
        self.date_when_new_announcement = {}
        self.rpt_period_of_new_announcement = {}
        
        for stk in sorted(self.all_stock_code):
            print stk
            date_list = []
            date_set = set()
            for tbname in ['balance_sheet','income_statement','cashflow_statement']:
                tbname = 'financial_data_'+tbname
                _dates = self.table_df[tbname].loc[stk].index#.index.get_level_values('Date').unique()
                _set = set(_dates)
                date_set = date_set|_set
            date_list = sorted(list(date_set))
            self.date_when_new_announcement[stk] = date_list
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=500) as executor:
            for stk in self.all_stock_code:
                func = self.match_date_and_reporting_period
                executor.submit(func, self.conn, stk, 150)
        #for stk in self.all_stock_code:
        #    self.match_rpt_period_with_date(stkcode, effective_num_day)
                
                
    #----------------------------------------------------------------------
    def match_rpt_period_with_date(self, stkcode, effective_num_day):
        """"""
        print stkcode
        df1 = self.table_df['financial_data_balance_sheet']
        df2 = self.table_df['financial_data_income_statement']
        df3 = self.table_df['financial_data_cashflow_statement']
        tb1 = df1[df1['StkCode']==stkcode]
        tb2 = df2[df2['StkCode']==stkcode]
        tb3 = df3[df3['StkCode']==stkcode]
        print tb1
        
        self.rpt_period_of_new_announcement[stkcode] = {}
        for date in self.date_when_new_announcement[stkcode]:
            rpt_period_list = []
            _date = datetime.strptime(row[0], '%Y%m%d')
            _rpt_limit = _date - datetime.strptime(days=effective_num_day)
            rpt_limit = _rpt_limit.strftime('%Y%m%d')            
            for tb in [tb1,tb2,tb3]:
                mask = (tb['Date']<=date)&(tb['ReportingPeriod']>=rpt_limit)
                rows = tb[mask].sort('Date',ascending=False).head(1)
                if rows.size > 0:
                    rtp_prd = rows['ReportingPeriod'].values[0]
                    company_type = rows['Company_type'].values[0]
                    rpt_period_list.append(rpt_prd)              
            if len(rpt_period_list) == 3:
                if (rpt_period_list[0] == rpt_period_list[1]
                    and rpt_period_list[0] == rpt_period_list[2]):    
                    self.rpt_period_of_new_announcement[stkcode][date] = [rpt_period_list[0],company_type]
                
                
    #----------------------------------------------------------------------
    def process(self):
        """"""
        msg = "Start to process financial data"
        self.log.info(msg)
        
        cur = self.conn.cursor()

        for stk in self.all_stock_code:
            tm1 = time.time()
            for date in sorted(self.rpt_period_when_data_announce[stk].keys()):
                this_fin_qt = self.rpt_period_when_data_announce[stk][date][0]
                fin_qts = self._get_past_fin_quarters(this_fin_qt, 10)
                company_type = self.days_when_data_announce[stk][date][1]
                #print stk,date,fin_qts,company_type
                
                result_dict = {}
                for name in self.fin_item_names:
                    self.fin_item_algos[name].calc(self.conn, stk, date, 
                                                   fin_qts, result_dict)
                        
            tm2 = time.time()
            print stk,tm2-tm1
                
                
    #----------------------------------------------------------------------
    def process_concurrent(self):
        """"""
        msg = "Start to process financial data"
        self.log.info(msg)
        
        cur = self.conn.cursor()
        for stk in self.all_stock_code:
            tm1 = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
                for date in sorted(self.rpt_period_when_data_announce[stk].keys()):
                    this_fin_qt = self.rpt_period_when_data_announce[stk][date][0]
                    fin_qts = self._get_past_fin_quarters(this_fin_qt, 10)
                    company_type = self.days_when_data_announce[stk][date][1]
                    #print stk,date,fin_qts,company_type
                    result_dict = {} 
                    for name in self.fin_item_names:
                        func = self.fin_item_algos[name].calc
                        executor.submit(func, self.conn, stk, date,fin_qts, result_dict)
                        
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
                
