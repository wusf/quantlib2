#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/7
"""

import logging


#----------------------------------------------------------------------
def loghandler(filename):
    """"""
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s|%(name)s|] %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    return (fh, ch)