#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/27
"""

import sqlite3 as lite


#----------------------------------------------------------------------
def load_data_into_memory_db(local_db_address, 
                             table_name_list,
                             index_name_str, 
                             date_col_name, date):
    """"""
    conn = lite.connect(':memory:', check_same_thread = False)
    lite.enable_shared_cache(0)
    conn.text_factory = str
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=OFF")
    cur.execute("PRAGMA read_uncommitted=1")
    
    cur.execute("attach '{}' as local_db".format(local_db_address))
    
    for tb in table_name_list:
        cur.execute("create table {} as select * from local_db.{} where {}>='{}'".format(tb, tb, date_col_name, date))
    cur.execute("detach local_db")
    
    for tb in table_name_list:
        cur.execute("create index {} on {} ({})".format(tb+'_index', tb, index_name_str))
    
    return conn