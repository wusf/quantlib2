#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/26
"""

#----------------------------------------------------------------------
def calc(conn, stkcode, date, fin_quarters, result_dict):
    """"""
    name = __file__.split('\\')[-1].split('.')[0]
    
    cur =conn.cursor()
    sql = """
          select NetProfitToParent
          from financial_data_income_statement
          where StkCode='{}'
          and ReportingPeriod='{}'
          and Date<='{}'
          order by Date desc
          """
    cur.execute(sql.format(stkcode, fin_quarters[0], date))
    res = cur.fetchone()
    if res is None:
        result_dict[name] = None
        return 0
    if res[0] is None:
        result_dict[name] = None
        return 0
    val1 = res[0]
    
    sql = """
          select NetCFO
          from financial_data_cashflow_statement
          where StkCode='{}'
          and ReportingPeriod='{}'
          and Date<='{}'
          order by Date desc
          """
    cur.execute(sql.format(stkcode, fin_quarters[0], date))
    res = cur.fetchone()
    if res is None:
        result_dict[name] = None
        return 0
    if res[0] is None:
        result_dict[name] = None
        return 0
    val2 = res[0]   
    
    result_dict[name] = val1 - val2
    return 1
    
    
    