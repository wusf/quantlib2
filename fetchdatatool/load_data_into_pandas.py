#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/28
"""

import sqlite3 as lite
import pandas as pd


#----------------------------------------------------------------------
def load_data_into_pandas(local_db_address_or_conn, 
                          table_names,
                          date_col_name, date):
    """"""
    if type(local_db_address_or_conn) == str:
        conn = lite.connect(local_db_address_or_conn)
    else:
        conn = local_db_address_or_conn
    sql = """
          select * from {} 
          where {}>='{}'
          """
    
    df_dict = {}
    for tb in table_names:
        _df = pd.read_sql(sql.format(tb, date_col_name, date),
                          conn, index_col=['StkCode','Date','ReportingPeriod'])
        df_dict[tb] = _df.sort_index()
    return df_dict



if __name__ == "__main__":
    addr = "D:\\quantdb\\rawdata.db"
    table_name_list = ['financial_data_Balance_Sheet',
                       'financial_data_Income_Statement',
                       'financial_data_CashFlow_Statement']    
    date_col_name = 'Date'
    date = '20070101'
    
    pl = load_data_into_pandas(addr, table_name_list, date_col_name, date)
    df = pl['financial_data_Balance_Sheet']
    
    #a = (df['StkCode']=='600837')
    #print df[a].sort('Date', ascending=False).head(1).size
    print df[df.Date=='600837']