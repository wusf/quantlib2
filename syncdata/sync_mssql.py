#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wushifan221@gmail.com --<>
  Purpose: 
  Created: 2016/7/8
"""

import os
import numpy
import logging
import codecs
import sqlite3
import pyodbc
from ConfigParser import ConfigParser
import datetool.slicedate as slicedate
import syncdata.sync as sync


########################################################################
class SyncRawDataFromMssql(sync.SyncRawData):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbname, dbinfocfgpath, tableinfocfgpath, 
                 startdate, log=None):
        """Constructor"""
        super(SyncRawDataFromMssql, self).__init__( dbname, dbinfocfgpath, tableinfocfgpath, 
                 startdate, log)
        
    
    #----------------------------------------------------------------------
    def conn_remote_db(self):
        """"""
        super(SyncRawDataFromMssql, self).conn_remote_db()
    
        conn_str = """
                       DRIVER={};
                       SERVER={};
                       DATABASE={};
                       UID={};
                       PWD={};
                       """.format(self.dbdriver, 
                                  self.dbserver, self.dbname, 
                                  self.dbuser, self.dbpassword)
        self.remote_conn = pyodbc.connect(conn_str)
    
        msg = 'Connected successfully'
        self.log.info(msg)