#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose:
  Created: 2016/7/13
"""

import quantlib as qt

########################################################################
class DefineEvent(qt.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log):
        """Constructor"""
        super(DefineEvent, self).__init__(dbinfocfgpath, log)
        
        msg = "Initiate class DefineEvent"
        self.log.info(msg)
        
        self.start_date = ''
        self.end_date = ''
        
        
    #----------------------------------------------------------------------
    def define(self):
        """"""
        msg = "Start to define event"
        self.log.info(msg)
    