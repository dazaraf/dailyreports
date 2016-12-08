# -*- coding: utf-8 -*-
"""
Created on Wed Dec 07 10:58:32 2016

@author: USER 1
"""

import MySQLdb
import pandas as pd
db = MySQLdb.connect(user ='dudu', passwd = '12041992')
cursor = db.cursor()

#s2 = '''select indicators.date from research.indicators where indicators.Name = 'nadav'  and indicators.VAR100 is null  and  indicators.date in (select positionshist.date  from mayan.positionshist where positionshist.portfolio = 'U1320135' group by positionshist.date)'''
#cursor.execute(s2)
#dateVar100 = cursor.fetchall()
#dateVar100 = [x[0] for x in dateVar100]
#dateVar100df = pd.DataFrame(dateVar100)
#dateVar100df[1] = 'Var100'

print ''
##this function 
def generateReportFromPositionHist(positionsIndicatorsList = [], * args):
    
    d = []
    for indicator in positionsIndicatorsList:
    
        indicatorString = indicator
        query = ''' select indicators.date from research.indicators where indicators.Name = 'nadav'  and indicators.''' + indicator + ''' is null  and  
        indicators.date in (select positionshist.date  from mayan.positionshist where positionshist.portfolio = 'U1320135' group by positionshist.date)'''
        cursor.execute(query)
        indicator = cursor.fetchall()
        indicator = [x[0] for x in indicator]
        indicator = pd.DataFrame(indicator, columns = ['Date'])
        indicator[1] = "%s" %indicatorString
        indicator = indicator.set_index('Date', drop = False)
        d.append(indicator)
        
    dfinal = reduce(lambda df1, df2: pd.merge(df1, df2, on ='Date'), d)
    dfinal['Col'] = dfinal.ix[:,1].map(str)
    for i in range(2,len(d) + 1):
        dfinal['Col'] = dfinal['Col'].map(str) + ' ,' + dfinal.ix[:,i]
    
    col_index = len(d) + 1
    dfinal = dfinal.iloc[:,[0,col_index]]
    dfinal = dfinal.set_index('Date', drop = True)
    
    return dfinal
    
    

        
