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
import h5py
import time
from datetime import datetime
from datetime import timedelta
import numpy as np
import threading
lock = threading.Lock()
import concurrent.futures
from ConfigParser import ConfigParser
import pandas as pd
import quantlib as qt
import fetchdatatool.load_data_into_memory_db as ldim


########################################################################
class GenerateFundamentalFactor(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, start_date, dbinfocfgpath, log):
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
        msg = "Load database into in-memory database"
        self.log.info(msg)
        
        self.raw_conn.close()
        self.prc_conn.close()
        
        load2mem = ldim.LoadDataIntoMemory()
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        loc_db_raw_address = loc_db_raw_path+loc_db_raw_name        
        
        loc_db_prc_path = self.dbinfocfg.get('dblocal_processed', 'path')
        loc_db_prc_name = self.dbinfocfg.get('dblocal_processed', 'dbname')        
        db_loc_prc_address = loc_db_prc_path + loc_db_prc_name
        
        table_name_list = ['market_data_a_share2']
        index_name_str = 'StkCode,Date'
        date_col_name = 'Date'
        start_date = '20070101'
        
        load2mem.load(loc_db_raw_address,
                      table_name_list,
                      index_name_str,
                      date_col_name, start_date)
        
        table_name_list = ['financial_data']
        index_name_str = 'StkCode,Date'
        date_col_name = 'Date'
        start_date = '20070101'
        
        load2mem.load(db_loc_prc_address,
                      table_name_list,
                      index_name_str,
                      date_col_name, start_date)
        
        self.raw_conn = load2mem.conn
        self.prc_conn = load2mem.conn
        
    
    #----------------------------------------------------------------------
    def search_for_all_stock_codes(self):
        """"""
        msg = "Seach for all stock codes ever listed"
        self.log.info(msg)
        
        msg = "Data start date: {}".format(self.start_date)
        self.log.info(msg)
        
        cur = self.raw_conn.cursor()
        sql = """
              select distinct StkCode
              from market_data_a_share2
              where Date>='{}'
              """.format(self.start_date)
        cur.execute(sql)
        rows = cur.fetchall()
        self.all_stock_code = []
        for row in rows:
            self.all_stock_code.append(row[0])
        
        n = len(self.all_stock_code)
        msg = "{} stocks in total".format(n)
        self.log.info(msg)            
            
                    
    #----------------------------------------------------------------------
    def load_factor_algorithm(self, algo_file_path):
        """"""
        msg = "Load factor algorithms"
        self.log.info(msg)    
        
        fct_algo_cfg = ConfigParser()
        fct_algo_cfg.optionxform = str
        fct_algo_cfg.readfp(codecs.open(algo_file_path, 'r', 'utf-8-sig'))
        self.fct_style = fct_algo_cfg.sections()
        self.fct_name = []
        self.fct_algos = {}
        
        msg = "Factors:"
        self.log.info(msg)
        for style in self.fct_style:
            for name in fct_algo_cfg.options(style):
                msg = style + ': '+name
                self.log.info(msg)
                self.fct_name.append(name)
                self.fct_algos[name] = fct_algo_cfg.get(style, name)
                
        n = len(self.fct_name)
        msg = "{} factors in total".format(n)
        self.log.info(msg)            
            
            
    #----------------------------------------------------------------------
    def _create_factor_database(self, tb_name):
        """"""
        msg = "Create fundamental factor database"
        self.log.info(msg)    
        
        loc_db_prc_path = self.dbinfocfg.get('dblocal_processed', 'path')
        loc_db_prc_name = self.dbinfocfg.get('dblocal_processed', 'dbname')        
        db_loc_prc_address = loc_db_prc_path + loc_db_prc_name
        self.fct_conn = lite.connect(db_loc_prc_address, check_same_thread = False)
        self.fct_conn.text_factory = str
        
        cur = self.fct_conn.cursor()
        sql = "drop table if exists {}".format(tb_name)
        cur.execute(sql)
        sql = "drop index if exists {}".format('index_'+tb_name)
        cur.execute(sql)
        cur.execute("PRAGMA synchronous = OFF")
        
        sql = """
              create table {}(StkCode text,
                              Date text
                              {})
              """
        initial_str = ""
        for factor in self.fct_name:
            _str = ','+factor+' float'
            initial_str+=_str
        cur.execute(sql.format(tb_name, initial_str))
        
        
    #----------------------------------------------------------------------
    def generate_to_hdf5(self):
        """"""
        msg = "Generating factors and store in hdf5 file"
        self.log.info(msg)        
        
        store = pd.HDFStore('factors.h5')
        tm1 = time.time()
        
        for stk in self.all_stock_code:
            self._calculate_factor_value_by_sql(stk, store)
        tm2 = time.time()
        print tm2-tm1
       
        
    #----------------------------------------------------------------------
    def generate_to_sql(self, tb_name):
        """"""
        msg = "Generating factors and store in sqlite database"
        self.log.info(msg)        
        store = ""
        
        self._create_factor_database(tb_name)
        
        cur = self.fct_conn.cursor()
        
        tm1 = time.time()
        q_str = '?,?'+len(self.fct_name)*',?'
        for stk in self.all_stock_code:
            df = self._calculate_factor_value_by_pd(stk, store)
            n = len(df)
            date = df.index.get_values().reshape(n,1)
            stkcode = np.array([[stk]]*n)
            data = np.hstack((stkcode,date,df.values))
            for row in data:
                cur.execute("""
                            insert into {} 
                            values ({})
                            """.format(tb_name,q_str), tuple(row))
        self.fct_conn.commit()
        tm2 = time.time()
        msg = "Time consumed: {}s".format(round(tm2-tm1, 0))
        self.log.info(msg)
        cur.execute("create index {} on {}(StkCode,Date)".format('index_'+tb_name,tb_name))
        msg = "Create index for factor table"
        self.log.info(msg)
        

    #----------------------------------------------------------------------
    def _calculate_factor_value_by_pd(self, stkcode, store):
        """"""
        msg = "Processing stock {}".format(stkcode)
        self.log.info(msg)
        
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
        
        data = pd.concat([df_mkt,df_fin], axis=1)
        data = data.fillna(method='ffill')
        
        vals = []
        for name in self.fct_name:
            _val = eval(self.fct_algos[name])
            vals.append(_val.to_frame(name))
        result = pd.concat(vals, axis=1) 
        #store['val_'+stkcode] = result
        return result
    
    
    #----------------------------------------------------------------------
    def _calculate_factor_value_by_sql(self, stkcode, store):
        """"""
        raw_cur = self.raw_conn.cursor()
        sql = """
              select market_data_a_share2.Date,ClosePrice,AFloatShare,TotalShare,financial_data.*
              from market_data_a_share2
              left join financial_data on financial_data.StkCode=market_data_a_share2.StkCode 
              and financial_data.Date<=market_data_a_share2.Date
              where market_data_a_share2.StkCode='{}' and market_data_a_share2.Date>='{}'
              group by market_data_a_share2.Date
              """.format(stkcode, self.start_date)
        df = pd.read_sql(sql, self.raw_conn, index_col='Date')
        
        data = df.fillna(method='ffill')
        
        vals = []
        for name in self.fct_name:
            _val = eval(self.fct_algos[name])
            vals.append(_val.to_frame(name))
        result = pd.concat(vals, axis=1) 
        return result
    
        
    #----------------------------------------------------------------------
    def multi_calc_func(self, stks, store):
        """"""
        for stk in stks:
            self._calculate_factor_value(stk, store)
            
            
    #----------------------------------------------------------------------
    def parallel_generate(self):
        """"""
        msg = "Generating factors"
        self.log.info(msg)        
        
        store = pd.HDFStore('factors.h5')
        tm1 = time.time()
        
        allStkCodes = self.all_stock_code
        thread_num = 10
        threads = []
        for i in xrange(thread_num):
            if i != thread_num:
                stks=allStkCodes[i*(len(allStkCodes)/thread_num):(i+1)*(len(allStkCodes)/thread_num)]
            else:
                stks=allStkCodes[i*(len(allStkCodes)/thread_num):]
            t = threading.Thread(target=self.multi_calc_func, args=(stks,store))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            

        tm2 = time.time()
        print tm2-tm1        
            
            

        
        

