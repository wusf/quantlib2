#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/7
"""

import os
import time
import numpy
import logging
import codecs
import sqlite3
import pyodbc
from ConfigParser import ConfigParser
import datetool.slicedate as slicedate
import quantlib as qt
os.environ['NLS_LANG'] = "SIMPLIFIED CHINESE_CHINA.UTF8"


########################################################################
class SyncRawData(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbname, dbinfocfgpath, tableinfocfgpath, 
                 startdate, log=None):
        """Constructor"""
        super(SyncRawData, self).__init__(dbinfocfgpath, log)
            
        msg = "Initiate program"
        self.log.info(msg)
            
        self.start_date = startdate
        self.sd = slicedate.SliceDate()

        self.dbdriver = self.dbinfocfg.get(dbname, 'dbdriver')
        self.dbname = self.dbinfocfg.get(dbname, 'dbname')
        self.dbserver = self.dbinfocfg.get(dbname, 'server')
        self.dbuser = self.dbinfocfg.get(dbname, 'user')
        self.dbpassword = self.dbinfocfg.get(dbname, 'password')
        self.loc_db_path = self.dbinfocfg.get('dblocalraw', 'path')
        self.loc_db_name = self.dbinfocfg.get('dblocalraw', 'dbname')
        
        tableinfocfg = ConfigParser()
        tableinfocfg.optionxform = str
        tableinfocfg.readfp(codecs.open(tableinfocfgpath, 'r', 'utf-8-sig'))
        self.tables = tableinfocfg.options('tables')
        self.tb_fields = {}
        self.tb_create_commands = {}
        self.tb_query_commands = {}
        self.tb_index = {}
        for tb in self.tables:
            self.tb_fields[tb] = tableinfocfg.items('fields_'+tb)
            self.tb_create_commands[tb] = tableinfocfg.get('commands_'+tb, 
            'create')
            self.tb_query_commands[tb] = tableinfocfg.get('commands_'+tb, 'query')
            self.tb_index[tb] = tableinfocfg.get('commands_'+tb, 'index')
        
        self.remote_conn = None
        self.local_conn = None
    
    
    #----------------------------------------------------------------------
    def conn_remote_db(self):
        """"""
        msg = "connecting to remote database '{}'".format(self.dbname)
        self.log.info(msg)
    
    
    #----------------------------------------------------------------------
    def conn_local_db(self):
        """"""
        msg = "Connecting to local database 'rawdata'"
        self.log.info(msg)
        
        self.local_conn = sqlite3.connect(self.loc_db_path+self.loc_db_name)
        self.local_conn.text_factory = str
        
        msg = 'Connected successfully'
        self.log.info(msg)

    
    #----------------------------------------------------------------------
    def _check_local_table(self, tbname):
        """"""
        msg = "Check table '{}'".format(tbname)
        self.log.info(msg)
        
        lastdate = ''
        tablename = ''
        cur = self.local_conn.cursor()
        sql_tbinfo = """
              select name from sqlite_master
              where type='table' and name='{}'
              """.format(tbname)
        cur.execute(sql_tbinfo)
        tbinfo = cur.fetchone()
        if tbinfo is not None:
            tablename = tbinfo[0]
            sql_tbdate = """
                         select distinct date from '{}'
                         order by date desc limit 1
                         """.format(tbname)
            cur.execute(sql_tbdate)
            tbdate = cur.fetchone()
            if tbdate is not None:
                lastdate = tbdate[0]
        return (tablename, lastdate)
        
    
    #----------------------------------------------------------------------
    def _drop_local_table(self, tbname):
        """"""
        msg = "Drop table '{}'".format(tbname)
        self.log.info(msg)
        
        cur = self.local_conn.cursor()
        sql = "drop table if exists {}".format(tbname)
        cur.execute(sql)
    
    
    #----------------------------------------------------------------------
    def _create_local_table(self, tbname):
        """"""
        msg = "Create table '{}'".format(tbname)
        self.log.info(msg)
        
        cur = self.local_conn.cursor()
        fields_str = ""
        for item in self.tb_fields[tbname]:
            fieldinfo = item[1].split('|')
            fieldstr = ','+fieldinfo[0]+' '+fieldinfo[1]
            fields_str += fieldstr
        sql = self.tb_create_commands[tbname].format(tbname, fields_str)
        cur.execute(sql)
    
        
    #----------------------------------------------------------------------
    def _fetch_and_insert(self, tbname, startdate, transfer_period, buffer_size):
        """"""
        msg = "Fetch data and insert to table '{}'".format(tbname)
        self.log.info(msg)
        
        cur_fetch = self.remote_conn.cursor()
        cur_insert = self.local_conn.cursor()

        cur_insert.execute("PRAGMA synchronous = OFF")

        columns_query = "PRAGMA table_info({})".format(tbname)
        cur_insert.execute(columns_query)
        num_columns = len(cur_insert.fetchall())
        
        field_str = ''
        for item in self.tb_fields[tbname]:
            fieldstr = ','+item[0]
            field_str += fieldstr
        
        date_segment = self.sd.run(transfer_period, startdate)
        
        for seg in sorted(date_segment):
            _startdate = date_segment[seg][0]
            _enddate = date_segment[seg][-1]
            sql_query = self.tb_query_commands[tbname].format(field_str, _startdate, _enddate)
            sql_insert = "insert or ignore into {} values (?{})".format(tbname, ',?'*(num_columns-1))
            
            tm1 = time.time()
            cur_fetch.execute(sql_query)
            tm2 = time.time()
            msg = "Transfer data from {} to {}".format(_startdate, _enddate)
            self.log.info(msg)
            querytime = tm2-tm1
            fetchtime = 0
            inserttime = 0
            while True:
                tm3 = time.time()
                rows = cur_fetch.fetchmany(buffer_size)
                tm4 = time.time()
                if not rows:
                    break
                tm5 = time.time()
                self.local_conn.executemany(sql_insert, rows)
                tm6 = time.time()
                fetchtime += (tm4-tm3)
                inserttime += (tm6-tm5)
            msg = ("Query time {}, fetch time {}, insert time {}".
                   format(querytime,fetchtime,inserttime))
            self.log.info(msg)
            
        msg = "Table '{}' has been synchronized".format(tbname)
        self.log.info(msg)
            
    
    #----------------------------------------------------------------------
    def _drop_index(self, tbname):
        """"""
        msg = "Drop index of table '{}'".format(tbname)
        self.log.info(msg)
        
        cur = self.local_conn.cursor()
        sql = "drop index if exists 'index_{}'".format(tbname)
        cur.execute(sql)
    
    
    #----------------------------------------------------------------------
    def _create_index(self, tbname):
        """"""
        msg = "Create index of table '{}'".format(tbname)
        self.log.info(msg)
        
        cur = self.local_conn.cursor()
        sql = "create index index_{} on {}({})".format(tbname,tbname,self.tb_index[tbname])
        cur.execute(sql)
    
    
    #----------------------------------------------------------------------
    def _update_local_table(self, tbname, transfer_period, buffer_size):
        """"""
        msg = "Update table '{}'".format(tbname)
        self.log.info(msg)
        
        self._drop_index(tbname)
        check = self._check_local_table(tbname)
        if check[0] != tbname:
            startdate = self.start_date
            self._create_local_table(tbname)
        else:
            startdate = check[1]
        
        if startdate == '':
            startdate = self.start_date
        self._fetch_and_insert(tbname, startdate, transfer_period, buffer_size)
        self._create_index(tbname)
        
        
    #----------------------------------------------------------------------
    def _rebuilt_local_table(self, tbname, transfer_period, buffer_size):
        """"""
        msg = "Rebuilt table '{}'".format(tbname)
        self.log.info(msg)
        
        self._drop_index(tbname)
        self._drop_local_table(tbname)
        startdate = self.start_date
        self._create_local_table(tbname)
        self._fetch_and_insert(tbname, startdate, transfer_period, buffer_size)
        self._create_index(tbname)
        
        
    #----------------------------------------------------------------------
    def run(self, transfer_period, buffer_size, method='rebuilt'):
        """"""
        msg = ("______Start to run synchonization, method={}______"
               .format(method))
        self.log.info(msg)
        
        if method == 'rebuilt':
            for tb in self.tables:
                self._rebuilt_local_table(tb, transfer_period, buffer_size)
        if method == 'update':
            for tb in self.tables:
                self._update_local_table(tb, transfer_period, buffer_size)













