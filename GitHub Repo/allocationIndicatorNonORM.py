# -*- coding: utf-8 -*-
"""
Created on Sun Dec 11 11:50:00 2016

@author: USER 1
"""

import MySQLdb
import pandas as pd
import numpy as np
import math
import datetime
import sys
import socket
import re


db = MySQLdb.connect(user ='dudu', passwd = '12041992', db = 'mayan')
cursor = db.cursor()

def read_historical_data_socket(sock, recv_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    data = ""
    while True:
        data = sock.recv(recv_buffer)
        buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break

    # Remove the end message string
    buffer = buffer[:-12]
    return buffer

    
def intraVol(begDate, endDate, equityList = [], *args):
    
    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port
#    
    begin = re.sub("-","", begDate)
    end = re.sub("-", "", endDate)
    
   
    message1 = "HIT,%s,600,%s 075000,%s 160000,,093000,160000,1\n" %(equityList[0],begin, end)
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    
    sock.sendall(message1)
    data1 = read_historical_data_socket(sock)
    
    data1 = "".join(data1.split("\r"))
    data1 = data1.replace(",\n","," + equityList[0] + "\n")[:-1]

    f = open("intraVol.csv", "w")
    f.write(data1)
    f.close()
    
    # Download each symbol to disk
    for sym in equityList[1:]:
        
        print "Downloading symbol: %s..." % sym

        # Construct the message needed by IQFeed to retrieve data
        message = "HIT,%s,600,%s 075000,%s 160000,,093000,160000,1\n" %(sym,begin, end)
        

        # Open a streaming socket to the IQFeed server locally
#        message = "HIT,AAPL,600,20160301 075000,20160601 160000,,093000,160000,1\n"


        # Send the historical data request
        # message and buffer the data
        sock.sendall(message)
        data = read_historical_data_socket(sock)
        sock.close

        # Remove all the endlines and line-ending
        # comma delimiter from each record
        data = "".join(data.split("\r"))
        data = data.replace(",\n","," + sym + "\n")[:-1]

        # Write the data stream to disk
        f2 = open("intraVol.csv", "a")
        f2.write(data)
        
        f2.close()
        
    df = pd.read_csv("intraVol.csv", error_bad_lines=False, header = None)
    df1 = df[[0,2,3,7]]
    df1.columns = ['Date', 'Low', 'High', 'Ticker']
    df1['Return'] = (df1['High'] - df1['Low'])/df1['Low']
    df1['Date'] = pd.to_datetime(df1['Date']).dt.date      
    
    df2 = df1.groupby(['Date','Ticker'])['Return'].std().reset_index()
    return df2

def listOfEquities(begDate, endDate, strat):
    
    query = "Select distinct(ticker) from positionshist join	portfolioshist on positionshist.portfolio = portfolioshist.name where positionshist.date between '" + begDate + "' and '" + endDate + "' and portfolioshist.user = '" + strat + "' "
    
    cursor.execute(query)
    
    args = cursor.fetchall()
    args = [x[0] for x in args]
    
    return args
    

def allocationIndicator(begDate, endDate, strat):
    
    equitiesList = listOfEquities(begDate, endDate, strat)
    
    ##query equities db
    
    sql = "Select Date, Ticker, Return1d from eqthist where eqthist.date between '" + begDate + "' and '"+ endDate +"' and ticker in (%s)"
    in_p = ', '.join(map(lambda x: '%s', equitiesList))
    sql = sql % in_p
    cursor.execute(sql, equitiesList)
    eqthist = cursor.fetchall()
    eqthist = pd.DataFrame(list(eqthist), columns = ['Date', 'Ticker', 'Return1d'])

    
    ##query positionshist db
    sql2 = "Select distinct positionshist.date, ticker, price, quantity from positionshist join	portfolioshist on positionshist.portfolio = portfolioshist.name where positionshist.date between '" + begDate + "' and '" + endDate + "' and portfolioshist.user = '" + strat + "' "
    cursor.execute(sql2)
    dfPositions = cursor.fetchall()
    dfPositions = pd.DataFrame(list(dfPositions), columns = ['Date', 'Ticker', 'Price', 'Qty'])
    dfPositions['PositionSize'] = dfPositions.Price * dfPositions.Qty

    ##add in the sumPositionSize
    dfPositions = dfPositions.join(dfPositions.groupby('Date')['PositionSize'].sum(), on = 'Date', rsuffix='Sum ')   
    
    ##count distinct tickers
    dfPositions = dfPositions.join(dfPositions.groupby(dfPositions['Date']).Ticker.nunique(), on = 'Date', rsuffix = 'Count')
    df = pd.merge(dfPositions, eqthist, on = ['Date', 'Ticker'])
    df['Return1d'] = np.where(df['Qty'] < 0, -df['Return1d'], df['Return1d'])
    
    df['ActualAllocation'] = df['PositionSize']/df.iloc[:,4]
    df['EqualAllocation'] = (1/df['TickerCount'])
    
    df['ActualContribution'] = df['ActualAllocation'] * df['Return1d']
    df['EqualContribution'] = df['EqualAllocation'] * df['Return1d']
    
    volatility = intraVol(begDate,endDate,equitiesList)
    df = pd.merge(df, volatility, on = ['Date', 'Ticker'])
    df['VolatilityContribution'] = df['ActualAllocation'] * df['Return']
    df = df.groupby(df['Date'])['EqualContribution', 'ActualContribution', 'VolatilityContribution'].sum()
    df['Indicator'] = (df['ActualContribution'] - df['EqualContribution'])/df['VolatilityContribution']
    df['logInd'] = np.where(df['Indicator'] > 0, np.log(df['Indicator'] + 1), - np.log(np.absolute(df['Indicator']) + 1) )
    
    return df
    
    