#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/13
"""

import logging
from ConfigParser import ConfigParser


########################################################################
class QuantLib(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log=None):
        """Constructor"""
        if log==None:
            self.log = logging.Logger('')
        else:
            self.log = log
            
        msg = "Initiate class QuantLib"
        self.log.info(msg)
        
        self.dbinfocfg = ConfigParser()
        self.dbinfocfg.read(dbinfocfgpath)
 

