#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/8/11
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
          select ifnull(FinExpns,InterestCost)
          from financial_data_income_statement
          where StkCode='{}'
          and ReportingPeriod='{}'
          and Date<='{}'
          order by Date desc
          """

    cur.execute(sql.format(stkcode, rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val1 = np.nan
    elif res[0] is None:
        val1 = np.nan
    else:
        val1 = res[0]

    cur.execute(sql.format(stkcode, last_ann_rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val2 = np.nan
    elif res[0] is None:
        val2 = np.nan
    else:
        val2 = res[0]

    cur.execute(sql.format(stkcode, last_same_rpt_period, date))
    res = cur.fetchone()
    if res is None:
        val3 = np.nan
    elif res[0] is None:
        val3 = np.nan
    else:
        val3 = res[0]

    cur.execute(sql.format(stkcode, ann_rpt_period_2y_ago, date))
    res = cur.fetchone()
    if res is None:
        val4 = np.nan
    elif res[0] is None:
        val4 = np.nan
    else:
        val4 = res[0]

    cur.execute(sql.format(stkcode, same_rpt_period_2y_ago, date))
    res = cur.fetchone()
    if res is None:
        val5 = np.nan
    elif res[0] is None:
        val5 = np.nan
    else:
        val5 = res[0]

    result_dict[name] = val1
    result_dict[name_ttm] = val1 + val2 - val3
    result_dict[name_1y_ago] = val3
    result_dict[name_ttm_1y_ago] = val3 + val4 - val5
    result_dict[name_2y_ago] = val5