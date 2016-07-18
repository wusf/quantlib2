#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/8
"""

import os
import numpy
import logging
import codecs
import sqlite3
import cx_Oracle
from ConfigParser import ConfigParser
import datetool.slicedate as slicedate
import syncdata.sync as sync


########################################################################
class SyncRawDataFromOracle(sync.SyncRawData):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbname, dbinfocfgpath, tableinfocfgpath, 
                 startdate, log=None):
        """Constructor"""
        super(SyncRawDataFromOracle, self).__init__( dbname, dbinfocfgpath, tableinfocfgpath, 
                 startdate, log)
        
    
    #----------------------------------------------------------------------
    def conn_remote_db(self):
        """"""
        super(SyncRawDataFromOracle, self).conn_remote_db()
        
        self.remote_conn = cx_Oracle.connect(self.dbuser, 
                                             self.dbpassword,
                                             self.dbserver)
    
        msg = 'Connected successfully'
        self.log.info(msg)