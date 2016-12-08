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
def generateReportFromPositionHist(strat, positionsIndicatorsList = [], * args):
    
    listOfDf = []
    
    if strat == 'nadav':
        
        for indicator in positionsIndicatorsList:
        
            query = '''select date
            from research.indicators
            where indicators.Name = 'nadav'  
            and indicators.''' + indicator + ''' is null  
            and indicators.date in
            (
             select date
             from mayan.positionshist where positionshist.ticker not in ('QSP.UN', 'SWY.CVR1',
            'SWY.CVR2') and  (positionshist.portfolio = 
                                (Select users.portfolio from mayan.users
                                where users.tradername =  'nadav') or positionshist.portfolio = 
                                (Select users.oldPortfolio from mayan.users
                                 where users.tradername =  'nadav') ) group by date
                                 )'''
            cursor.execute(query)
            ind = cursor.fetchall()
            ind = [x[0] for x in ind if len(x) !=0]
            listOfDf.append(ind)
            listOfDf = [x for x in listOfDf if len(x) !=0]
            

    else:
    
        for indicator in positionsIndicatorsList:
            
           query = "select date  from research.indicators   where indicators.Name ='" + strat + "'and indicators." + indicator + " is null and indicators.date in (select date   from mayan.positionshist where positionshist.portfolio =   (Select users.portfolio from mayan.users   where users.tradername =  '" + strat + "' ) or positionshist.portfolio = (Select users.oldPortfolio from mayan.users   where users.tradername = '" + strat + "' )  group by date  ) "
           cursor.execute(query)
           ind = cursor.fetchall()
           ind = [x[0] for x in ind if len(x) !=0]
           dfInd = pd.DataFrame(ind,columns = ['Date'] )
           dfInd[1] = '%s' %indicator
           listOfDf.append(dfInd)
           listOfDf= [x for x in listOfDf if len(x) !=0]
        
           
    if len(listOfDf) == 0:
        
        return "All Clear"
    
    else:
        
        dfinal = reduce(lambda df1, df2: pd.merge(df1, df2, on ='Date'), listOfDf)
        dfinal['Col'] = dfinal.ix[:,1].map(str)
        for i in range(2,len(listOfDf) + 1):
            dfinal['Col'] = dfinal['Col'].map(str) + ' ,' + dfinal.ix[:,i]
           
        col_index = len(listOfDf) + 1
        dfinal = dfinal.iloc[:,[0,col_index]]
        dfinal = dfinal.set_index('Date', drop = True)
    
        return dfinal
    
    

        
