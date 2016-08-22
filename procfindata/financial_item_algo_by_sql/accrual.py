#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/26
"""

import numpy as np


#----------------------------------------------------------------------
def calc(conn, stkcode, date, fin_quarters, company_type, result_dict):
    """"""
    name = __file__.split('\\')[-1].split('.')[0]
    name_ttm = name + '_ttm'
    name_1y_ago = name + '_1y_ago'
    name_ttm_1y_ago = name_ttm + '_1y_ago'
    name_2y_ago = name + '_2y_ago'
    
    rpt_period  = fin_quarters[0]
    
    last_year = str(int(fin_quarters[0][0:4])-1)
    last_ann_rpt_period = last_year + '1231'
    last_same_rpt_period = last_year + rpt_period[4:]
    
    year_2y_ago = str(int(fin_quarters[0][0:4])-2)
    ann_rpt_period_2y_ago = year_2y_ago + '1231'
    same_rpt_period_2y_ago = year_2y_ago + rpt_period[4:]    
    
    cur =conn.cursor()
    
    sql = """
          select NetProfitToParent
          from financial_data_income_statement
          where StkCode='{}'
          and ReportingPeriod='{}'
          and Date<='{}'
          order by Date desc
          """
    
    cur.execute(sql.format(stkcode, rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val11 = np.nan
    elif res[0] is None:
        val11 = np.nan
    else:
        val11 = res[0]
        
    cur.execute(sql.format(stkcode, last_ann_rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val21 = np.nan
    elif res[0] is None:
        val21 = np.nan
    else:
        val21 = res[0]
        
    cur.execute(sql.format(stkcode, last_same_rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val31 = np.nan
    elif res[0] is None:
        val31 = np.nan
    else:
        val31 = res[0]  
        
    cur.execute(sql.format(stkcode, ann_rpt_period_2y_ago, date))
    res = cur.fetchone()
    if res is None:
        val41 = np.nan
    elif res[0] is None:
        val41 = np.nan
    else:
        val41 = res[0]   
        
    cur.execute(sql.format(stkcode, same_rpt_period_2y_ago, date))
    res = cur.fetchone()
    if res is None:
        val51 = np.nan
    elif res[0] is None:
        val51 = np.nan
    else:
        val51 = res[0]    
    
    sql = """
          select NetCFO
          from financial_data_cashflow_statement
          where StkCode='{}'
          and ReportingPeriod='{}'
          and Date<='{}'
          order by Date desc
          """
    
    cur.execute(sql.format(stkcode, rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val12  = np.nan
    elif res[0] is None:
        val12 = np.nan
    else:
        val12 = res[0]
    
    cur.execute(sql.format(stkcode, last_ann_rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val22  = np.nan
    elif res[0] is None:
        val22 = np.nan
    else:
        val22 = res[0]
        
    cur.execute(sql.format(stkcode, last_same_rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val32  = np.nan
    elif res[0] is None:
        val32 = np.nan
    else:
        val32 = res[0]
        
    cur.execute(sql.format(stkcode, ann_rpt_period_2y_ago, date))
    res = cur.fetchone()
    if res is None:
        val42  = np.nan
    elif res[0] is None:
        val42 = np.nan
    else:
        val42 = res[0]
        
    cur.execute(sql.format(stkcode, same_rpt_period_2y_ago, date))
    res = cur.fetchone()
    if res is None:
        val52  = np.nan
    elif res[0] is None:
        val52 = np.nan
    else:
        val52 = res[0]
        
    result_dict[name] = val11 - val12
    result_dict[name_ttm] = (val11 - val12) + (val21 - val22) - (val31 - val32)
    result_dict[name_1y_ago] = val31 - val32
    result_dict[name_ttm_1y_ago] = (val31 - val32) + (val41 - val42) - (val51 - val52)
    result_dict[name_2y_ago] = val51 - val52
    
    
    