#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/7
"""

import datetime


########################################################################
class SliceDate(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, datelist=None):
        """Constructor"""
        self.date_list = datelist
        
        
    #----------------------------------------------------------------------
    def _slice_by_week(self, date_list):
        """"""
        startdate = date_list[0]
        startyear = date_list[0][0:4]
        d = datetime.datetime.strptime(startdate,'%Y%m%d')
        _startweek = (int(startyear)*100
                     +int(datetime.datetime.strftime(d,'%W')))
        startweek = str(_startweek)
        weekdict = {}
        
        thisweek = startweek
        weekdict[thisweek] = []
        for dt in date_list:
            d = datetime.datetime.strptime(dt,'%Y%m%d')
            year = dt[0:4]
            _week = int(datetime.datetime.strftime(d,'%W'))
            week = str(int(year)*100+_week)
            if week != thisweek:
                thisweek = week
                weekdict[thisweek] = []
            weekdict[thisweek].append(dt)
        return weekdict 
            
    
    #----------------------------------------------------------------------
    def _slice_by_month(self, date_list):
        """"""
        startmonth = date_list[0][0:6]
        monthdict = {}
        
        thismonth = startmonth
        monthdict[thismonth] = []
        for dt in date_list:
            _month = dt[0:6]
            if _month != thismonth:
                thismonth = _month
                monthdict[thismonth] = []
            monthdict[thismonth].append(dt)
        return monthdict
            
            
    #----------------------------------------------------------------------
    def _slice_by_quarter(self, date_list):
        """"""
        startquarter = (date_list[0][0:4]+'q'
                        +str(int(date_list[0][4:6])/4+1))
        quarterdict = {}
        
        thisquarter = startquarter
        quarterdict[thisquarter] = []
        for dt in date_list:
            _quarter = dt[0:4]+'q'+str(int(dt[4:6])/4+1)
            if _quarter != thisquarter:
                thisquarter = _quarter
                quarterdict[thisquarter] = []
            quarterdict[thisquarter].append(dt)
        return quarterdict  
    
    
    #----------------------------------------------------------------------
    def _slice_by_year(self, date_list):
        """"""
        startyear = date_list[0][0:4]
        yeardict = {}
        
        thisyear = startyear
        yeardict[thisyear] = []
        for dt in date_list:
            _year = dt[0:4]
            if _year != thisyear:
                thisyear = _year
                yeardict[thisyear] = []
            yeardict[thisyear].append(dt)
        return yeardict
        
        
    #----------------------------------------------------------------------
    def run(self, slice_method, start_date):
        """"""
        if self.date_list is None:
            today = datetime.date.today().strftime('%Y%m%d')
            datelist = gen_date_list(start_date, today)
        else:
            datelist = []
            for dt in self.date_list:
                if dt >= start_date:
                    datelist.append(dt)
        if slice_method == 'week':
            return self._slice_by_week(datelist)        
        if slice_method == 'month':
            return self._slice_by_month(datelist)
        if slice_method == 'quarter':
            return self._slice_by_quarter(datelist)
        if slice_methid == 'year':
            return self._slice_by_year(datelist)
        
        
        
#----------------------------------------------------------------------
def gen_date_list(starttime,endtime):
    startdate = datetime.datetime(int(starttime[0:4]),int(starttime[4:6]),int(starttime[6:8]))
    #now = datetime.datetime.now()
    delta = datetime.timedelta(days=1)
    # my_yestoday = startdate + delta
    # my_yes_time = my_yestoday.strftime('%Y%m%d')
    n = 0
    date_list = []
    while 1:
        if starttime<=endtime:
            days = (startdate + delta*n).strftime('%Y%m%d')
            n = n+1
            date_list.append(days)
            if days == endtime:
                break
    return date_list





if __name__ == '__main__':
    sd = SliceDate(None)
    seg = sd.run('week', '20150506')
    for _seg in sorted(seg):
        print _seg, seg[_seg][0],seg[_seg][-1]