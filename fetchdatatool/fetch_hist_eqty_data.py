#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/14
"""

import sys
import sqlite3 as lite
import quantlib as qt
import pandas as pd
import numpy as np


########################################################################
class FetchHistData(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log):
        """Constructor"""
        super(FetchHistData, self).__init__(dbinfocfgpath, log)
        
        msg = "Initiate class 'FetchHistData'"
        self.log.info(msg)
        
        loc_db_raw_path = self.dbinfocfg.get('dblocalraw', 'path')
        loc_db_raw_name = self.dbinfocfg.get('dblocalraw', 'dbname')        
        db_address = loc_db_raw_path+loc_db_raw_name
        self.conn = lite.connect(db_address)
        self.conn.text_factory = str
        
        msg = "Connect to local raw database"
        self.log.info(msg)
        
        
    #----------------------------------------------------------------------
    def fetch_close_price(self, stkcode_list, security_type,
                          start_date, end_date, is_adj):
        """"""
        tbname = ''
        fieldname = ''
        if security_type == 'a_stock':
            tbname = 'market_data_a_share'  
        elif security_type == 'index':
            tbname = 'market_data_index'
        elif security_type == 'etf':
            tbname = 'market_data_etf'
        else:
            msg = "Cannot find security type '{}'".format(security_type)
            self.log.info(msg)
            sys.exit()
            
        if is_adj == 0:
            fieldname = 'ClosePrice'
        elif is_adj == 1:
            fieldname = 'ClosePrice_adj'
        else:
            msg = "Price type is wrong, input type is {}".format(is_adj)
            self.log.info(msg)
            sys.exit()            
            
        
        cur = self.conn.cursor()
        df = pd.DataFrame()
        for stk in stkcode_list:
            date = []
            price = []
            sql = """
                  select Date,{} from {} 
                  where Date>='{}' and Date<='{}'
                  and StkCode='{}'
                  """.format(fieldname, tbname,
                             start_date, end_date, stk)
            cur.execute(sql)
            rows = cur.fetchall()
            for row in rows:
                date.append(row[0])
                price.append(row[1])
            _df = pd.DataFrame(price, index=date, columns=[stk])
            df = pd.concat([df,_df], axis=1)
        return df
    
    
    #----------------------------------------------------------------------
    def fetch_return(self, stkcode_list, security_type, 
                     start_date, end_date, period, return_type):
        """"""
        _price = self.fetch_close_price(stkcode_list, security_type, 
                                        start_date, end_date, 1)
        price = _price[::period]
        if return_type == 'log':
            logprice = np.log(price)
            df = logprice.diff(1)
        else:
            dff = price.diff(1)
            df = dff/price
        return df[1:]
    
    
    
    
    
    
    
    
    
    
    
