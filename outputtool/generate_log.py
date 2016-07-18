#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@sina.com>
  Purpose: 
  Created: 2016/7/11
"""

import logging 


########################################################################
class GenLog(logging.Logger):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, log_name, level):
        """Constructor"""
        self.log = logging.Logger(log_name, level)
        self.fh = logging.FileHandler(log_name+'.log')
        self.fh.setLevel(level)
        self.ch = logging.StreamHandler()
        self.ch.setFormatter(level)
        formatter = logging.Formatter('[%(asctime)s|%(name)s|] %(message)s',
                                      '%Y-%m-%d %H:%M:%S')
        self.fh.setFormatter(formatter)
        self.ch.setFormatter(formatter)

        
    #----------------------------------------------------------------------
    def _add_handlers(self, fh, ch):
        """"""
        if fh == 1:
            self.log.addHandler(self.fh)
        if ch == 1:
            self.log.addHandler(self.ch)
    
    
    #----------------------------------------------------------------------
    def generate(self, fh, ch):
        """"""
        self._add_handlers(fh, ch)
        return self.log
    