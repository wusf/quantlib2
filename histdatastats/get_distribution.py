#!/usr/bin/env python
#coding:utf-8
"""
  Author:  wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/7/15
"""

import sys
import quantlib as lib
import numpy as np
import scipy as sp
from scipy import stats
import fetchdatatool.fetch_hist_eqty_data as fd
import matplotlib.pylab as plt
import matplotlib.mlab as mlab
from scipy.stats import norm


########################################################################
class GetHistReturnDist(lib.QuantLib):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dbinfocfgpath, log):
        """Constructor"""
        super(GetHistReturnDist, self).__init__(dbinfocfgpath, log)
    
        msg = "Initiate class 'GetHistReturnDist'"
        self.log.info(msg)
        
        self.fetchdata = fd.FetchHistData(dbinfocfgpath, self.log)
        
        
    #----------------------------------------------------------------------
    def get_dist(self, secrurity_id, security_type, return_period,
                 return_type, start_date, end_date, num_bins):
        """"""
        retdf = self.fetchdata.fetch_return([secrurity_id], security_type, 
                                          start_date, end_date, 
                                          return_period, return_type)
        ret = retdf[secrurity_id].values
        density, bins = np.histogram(ret, bins=num_bins, density=True)
        bins = bins[:-1] + (bins[1] - bins[0])/2.0
        bins = np.round(bins, 3)
        density = np.round(density, 3)
        
        mu = np.nanmean(ret)
        sigma = np.nanstd(ret)
        skew = stats.skew(ret)
        kurt = stats.kurtosis(ret, fisher=False)
        statistics = np.round([mu,sigma,skew,kurt], 3)
        
        self.security_id = secrurity_id
        self.startdate = start_date
        self.enddate = end_date
        
        self.bins = bins
        self.density = density
        self.mu = statistics[0]
        self.sigma = statistics[1]
        self.sk = statistics[2]
        self.ku = statistics[3]
    
    
    #----------------------------------------------------------------------
    def compare2norm(self):
        """"""
        x =np.linspace(self.bins[0], self.bins[-1], 100)
        fig, ax = plt.subplots(1,1)
                      
        normdensity = mlab.normpdf(x, self.mu, self.sigma)
        ax.plot(x, normdensity, 'r--')
        ax.plot(self.bins, self.density)
        ax.text(0.1,0.6,'$mu={}$\n$sigma={}$\n$sk={}$\n$ku={}$'.
                format(self.mu,self.sigma,self.sk,self.ku),fontsize=15,
                transform=ax.transAxes)
        plt.grid()
        plt.title("Empirical vs. Gaussion\n{}'{}' {}'{}'-'{}'"
                 .format('StockCode:',self.security_id, 
                         'Date:', self.startdate, self.enddate))
        plt.xlabel('return')
        plt.ylabel('Probability distribution density')
        plt.legend(['Gaussion PDF', 'Empirical PDF'])
        plt.show()
