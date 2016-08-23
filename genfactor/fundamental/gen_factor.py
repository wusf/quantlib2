#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/8/22
"""

import sys
import codecs
import sqlite3 as lite
import time
from datetime import datetime
from datetime import timedelta
import concurrent.futures
from ConfigParser import ConfigParser
import pandas as pd
import quantlib as qt
import fetchdatatool.load_data_into_memory_db as loaddb


########################################################################
class GenerateFundamentalFactor(qt.QuantLib, start_date, dbinfocfgpath, log):
    """"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(GenerateFundamentalFactor, self).__init__(dbinfocfgpath, log)
        
        msg = "Initiate class 'GenerateFundamentalFactor'"
        self.log.info(msg)    
    
        self.start_date = start_date
    
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_local_raw_address = loc_db_raw_path + loc_db_raw_name
        self.raw_conn = lite.connect(db_local_raw_address, check_same_thread = False)
        self.raw_conn.text_factory = str
    
        loc_db_prc_path = self.dbinfocfg.get('dblocal_processed', 'path')
        loc_db_prc_name = self.dbinfocfg.get('dblocal_processed', 'dbname')        
        db_loc_prc_address = loc_db_prc_path + loc_db_prc_name
        self.prc_conn = lite.connect(db_loc_prc_address, check_same_thread = False)
        self.prc_conn.text_factory = str
    
    
    #----------------------------------------------------------------------
    def load_local_db_into_memory(self):
        """"""
        self.raw_conn.close()
        self.prc_conn.close()
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        loc_db_address = loc_db_raw_path+loc_db_raw_name        
        
        loc_db_prc_path = self.dbinfocfg.get('dblocal_processed', 'path')
        loc_db_prc_name = self.dbinfocfg.get('dblocal_processed', 'dbname')        
        db_loc_prc_address = loc_db_prc_path + loc_db_prc_name
        
        table_name_list = ['market_data_a_share',
                           'market_data_capital_stock']
        index_name_str = 'StkCode,Date'
        date_col_name = 'Date'
        start_date = '20070101'
        
        self.raw_conn = loaddb.load_data_into_memory_db(loc_db_address,
                                                        table_name_list,
                                                        index_name_str,
                                                        date_col_name, date)
        
        table_name_list = ['financial_report_data']
        index_name_str = 'StkCode,Date'
        date_col_name = 'Date'
        start_date = '20070101'
        
        self.prc_conn = loaddb.load_data_into_memory_db(loc_db_address,
                                                        table_name_list,
                                                        index_name_str,
                                                        date_col_name, date)
    
    
    #----------------------------------------------------------------------
    def find_all_stock_codes(self):
        """"""
        msg = "Find all stock codes"
        self.log.info(msg)    
        
        cur = self.raw_conn.cursor()
        sql = """
              select distinct StkCode
              from market_data_a_share
              where Date>='{}'
              """.format(self.start_date)
        cur.execute(sql)
        rows = cur.fetchall()
        self.all_stock_code = []
        for row in rows:
            self.all_stock_code.append(row[0])
            
    
    #----------------------------------------------------------------------
    def generate(self):
        """"""
        msg = "Generating factors"
        self.log.info(msg)        
        
        for stk in self.all_stock_code:
            print stk
            
            
    #----------------------------------------------------------------------
    def _calculate_factor_values(self, stkcode, algo_file_path):
        """"""
        raw_cur = self.raw_conn.cursor()
        sql = """
              select Date,ClosePrice,AFloatShare,TotalShare
              from market_data_a_share2
              where StkCode='{}' and Date>='{}'
              """.format(stkcode, self.start_date)
        df_mkt = pd.read_sql(sql, self.raw_conn, index_col='Date')
        
        prc_cur = self.prc_conn.cursor()
        sql = """
              select *
              from financial_data
              where StkCode='{}' and Date>='{}'
              """.format(stkcode, self.start_date)
        df_fin = pd.read_sql(sql, self.prc_conn, index_col='Date')
        
        _raw_data = pd.concat([df_mkt,df_fin], axis=1)
        raw_data = _raw_data.fillna(method='ffill')