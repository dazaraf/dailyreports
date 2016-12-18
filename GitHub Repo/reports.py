# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 11:32:18 2016

@author: USER 1
"""

import MySQLdb
import pandas as pd
import datetime
from volAlert import volAlert

db = MySQLdb.connect(user ='dudu', passwd = '12041992')
cursor = db.cursor()

def volAlert(begDate, endDate, strat):
    
    queryA = "Select (Select date from mayan.portfolioshist prt2 where prt2.date > prt.date and prt2.user = '" + strat + "' order by prt2.date asc limit 1), date from mayan.portfolioshist prt where prt.user = '" + strat + "' and prt.date >'" + begDate + "' and prt.date <'" + endDate + "' "   
    cursor.execute(queryA)
    x = list(cursor.fetchall())
    xdf = pd.DataFrame(x, columns = ['Date', 'PortfolioDate'])
    
    query2 = " Select date from research.indicators where name = '" + strat + "' and vol1d is not null and date > '" + begDate + "'  and date < '" + endDate + "'"
    cursor.execute(query2)
    y = cursor.fetchall()
    y = [tup[0] for tup in y]
    ydf = pd.DataFrame(y, columns=['Date'])
    ydf['y'] = 1
     
    maindf = pd.merge(xdf, ydf, how='left', on = ['Date'])
    maindf = maindf.fillna(0)

    query3 = "select date from mayan.portfolioshist prt where user = '" + strat + "'  and date > '" + begDate + "'  and date < '" + endDate + "' and prt.date not in (select distinct pos.date from mayan.positionshist pos join mayan.portfolioshist prt on pos.portfolio = prt.name where prt.user = '" + strat + "' and pos.ticker not in ('QSP.UN', 'SWY.CVR1','SWY.CVR2'))order by date asc"         
    cursor.execute(query3)
    z = cursor.fetchall()        
    z = [tupl[0] for tupl in z]
    zdf = pd.DataFrame(z, columns=['PortfolioDate'])
    zdf['z'] = 1
     
    maindf = pd.merge(maindf, zdf, how = 'left',  on = ['PortfolioDate'])
    maindf = maindf.fillna(0)    
    flagA = pd.DataFrame(maindf[(maindf['y'] == 0) & (maindf['z'] == 0)]['Date']).reset_index(drop = True)
    flagB = pd.DataFrame(maindf[(maindf['y'] == 1) & (maindf['z'] == 1)]['Date']).reset_index(drop = True)
    flagA['Strategy'] = strat

    flagA['Alert'] = 'Vol1D absent. Positions/Trades entry populated'
    flagB['Alert'] = 'Vol 1D populated. Positions entry absent' 
     

##the trades table query
    queryPortfolio = "Select distinct date from mayan.portfolioshist where user = '" + strat + "' and date > '" + begDate + "'  and date < '" + endDate + "'"
    queryIndicators = "Select distinct date from research.indicators where Name = '" + strat + "' and date > '" + begDate + "'  and date < '" + endDate + "' and vol1d is not null"
    querytrades = " select distinct(TradeDate) from mayan.tradeshist join mayan.portfolioshist on tradeshist.Portfolio = portfolioshist.name where portfolioshist.user = '" + strat + "' and TradeDate > '" + begDate + "' and TradeDate < '" + endDate + "'"
  
    cursor.execute(queryPortfolio)
    port = cursor.fetchall()
    cursor.execute(queryIndicators)
    ind = cursor.fetchall()
    cursor.execute(querytrades)
    trades = cursor.fetchall()
    
    port = [tup[0] for tup in port]
    dfPort = pd.DataFrame(port, columns = ['Date'])
    trades = [tup[0] for tup in trades]
    dfTrades = pd.DataFrame(trades, columns = ['Date'])
    dfTrades['Trades'] = 1
    maindf2 = pd.merge(dfPort, dfTrades, on = ['Date'], how = 'left')

    ind = [tup[0] for tup in ind]
    dfInd = pd.DataFrame(ind, columns = ['Date'])
    dfInd['Indicator'] = 1
    maindf2 = pd.merge(maindf2, dfInd, on = ['Date'], how = 'left')
    maindf2 = maindf2.fillna(0)
    
    flagC = pd.DataFrame(maindf2[(maindf2['Trades'] == 0) & (maindf2['Indicator'] == 1)]['Date']).reset_index(drop = True)
    flagC['Alert'] = 'Vol1d populated. Trades entry absent'
    flagD = pd.DataFrame(maindf2[(maindf2['Trades'] == 1) & (maindf2['Indicator'] == 0)]['Date']).reset_index(drop = True)
    flagD['Alert'] = 'Vol1D absent. Positions/Trades entry populated'
    flagD['Strategy'] = strat
    
    flagE = pd.concat([flagA,flagD]).reset_index(drop = True).drop_duplicates()
    cols = [1,2,0]
    flagE = flagE.iloc[:,cols]

    flagF = pd.DataFrame(pd.merge(flagB,flagC, how = 'inner', on = ['Date'])['Date'])
    flagF['Strategy'] = strat
    flagF['Alert'] = 'Vol1d populated. Trades & positions entry absent'
    
    volFlags = pd.concat([flagE,flagF]).sort_values(by = 'Date').reset_index(drop='True')
    
    return volFlags
    
def nadavReport(begDate, endDate, positionsIndicatorsList = [], * args):
    
    listOfDf = []
    
    
        
    for indicator in positionsIndicatorsList:
        
        query = '''select date
            from research.indicators
            where indicators.Name = 'nadav'  
            and indicators.''' + indicator + ''' is null
            and indicators.date >' ''' + begDate + ''' ' and indicators.date <' ''' + endDate + ''' '  
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
    dfinal['Strategy'] = 'nadav'

    cols = [0,2,1]
    dfinal = dfinal[cols]
    
    return dfinal                        

    
    
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
        
        if strat == 'nadav':
            df = nadavReport(begDate, endDate, positionsIndicatorsList)
        
        else:
            df = redFlagsHelper(begDate, endDate, strat, positionsIndicatorsList)
        listOfDf.append(df)
    
    finalReport = pd.concat(listOfDf).sort_values(by = 'Date').reset_index(drop = True)
    
    return finalReport

def volFlags(begDate, endDate, strats = [], * args):
    
    listOfDf = []
    for strat in strats:
        
        df = volAlert(begDate, endDate,strat)
        listOfDf.append(df)
    
    finalReport = pd.concat(listOfDf).sort_values(by = 'Date').reset_index(drop = True)
    
    return finalReport