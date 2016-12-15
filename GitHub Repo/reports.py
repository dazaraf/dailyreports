# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 11:32:18 2016

@author: USER 1
"""

import MySQLdb
import pandas as pd
import datetime
db = MySQLdb.connect(user ='dudu', passwd = '12041992')
cursor = db.cursor()

def redFlagsHelper(begDate, endDate, strat, indicators = [], * args):
    
    listOfDf = []
    for indicator in indicators:
        
       query = "Select date from research.indicators where Name ='" + strat + "' and " + indicator + " is null and date >' " + begDate + " ' and date <' " + endDate + " ' and date in (Select positionshist.date from mayan.positionshist join mayan.portfolioshist on positionshist.portfolio = portfolioshist.name where portfolioshist.user = '" + strat + "' group by positionshist.date)"
       cursor.execute(query)
       ind = cursor.fetchall()
       ind = [x[0] for x in ind if len(x) !=0]
       dfInd = pd.DataFrame(ind,columns = ['Date'] )
       dfInd['Indicator'] = '%s' %indicator
       listOfDf.append(dfInd)
       listOfDf= [x for x in listOfDf if len(x) !=0]
       
    if len(listOfDf) == 1:
        dfinal = listOfDf[0]
        dfinal.columns = ['Date','Indicator']

   
    dfinal = reduce(lambda df1, df2: pd.merge(df1, df2, how = 'outer', on ='Date'), listOfDf)
    dfinal = dfinal.fillna("")
    
    dfinal['Alert'] = dfinal.ix[:,1].map(str)
    
    for i in range(2,len(listOfDf) + 1):
        dfinal['Alert'] = dfinal['Alert'].map(str) + ' ,' + dfinal.ix[:,i]
    
    col_index = len(listOfDf) + 1
    dfinal = dfinal.iloc[:,[0,col_index]]
#   
    dfinal['Alert'] = dfinal['Alert'] + ' is/are null despite positions held'
    dfinal['Strategy'] = strat

    cols = [0,2,1]
    dfinal = dfinal[cols]
    
    return dfinal

def redFlags(begDate, endDate ,strats = [], positionsIndicatorsList = [], * args):
    
    listOfDf = []
    for strat in strats:
        
        df = redFlagsHelper(begDate, endDate, strat, positionsIndicatorsList)
        listOfDf.append(df)
    
    finalReport = pd.concat(listOfDf).sort_values(by = 'Date').reset_index(drop = True)
    
    return finalReport
        