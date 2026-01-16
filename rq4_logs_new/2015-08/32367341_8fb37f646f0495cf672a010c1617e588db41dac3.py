#! /usr/bin/env python
# -*- coding: utf-8 -*-

from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
from time import sleep
import time
from datetime import datetime
import collections
from threading import Timer

# define all global variable
bid, ask = 0.0, 0.0

tickMaxLen = 10000
vbid, vask = collections.deque(maxlen=tickMaxLen), collections.deque(maxlen=tickMaxLen)
_long, _short, _sell, _cover = False, False, False, False

maxLength = 500
hdata_1m = {'date': collections.deque(maxlen=maxLength), \
            'open': collections.deque(maxlen=maxLength), \
            'high': collections.deque(maxlen=maxLength), \
            'low':  collections.deque(maxlen=maxLength), \
            'close':collections.deque(maxlen=maxLength)}
hdata_2m = hdata_1m
hdata_5m = hdata_1m
hdata_15m = hdata_1m
hdata_30m = hdata_1m
hdata_1h = hdata_1m
hdata_1d = hdata_1m

# print all messages from TWS
def watcher(msg):
    print msg

def fxbidask(msg):
    if msg.field in [1, 2, 4, 6, 9]:
        dt = datetime.now()
        f2.writelines("%s|%d|%.5f\n" % (dt.strftime("%Y-%m-%d %H:%M:%S"), msg.field, msg.price))

def fxtick(msg):
    global bid, ask
    if msg.side == 0:
        ask = msg.price
    if msg.side == 1:
        bid = msg.price

    mid = (ask+bid)/2

    #curTime = time.time()
    #print curTime
    dt = datetime.now()
    #print "%s.%s|%.5f|%.5f" % (dt.strftime("%Y-%m-%d %H:%M:%S"), dt.microsecond, bid, ask)
    f1.writelines("%s|%.5f|%.5f\n" % (dt.strftime("%Y-%m-%d %H:%M:%S"), bid, ask))

# show Bid and Ask quotes
def my_BidAsk(msg):
    if msg.field == 1:
        print '%s:%s: bid: %s' % (contractTuple[0],
                       contractTuple[6], msg.price)
    elif msg.field == 2:
        print '%s:%s: ask: %s' % (contractTuple[0], contractTuple[6], msg.price)

def makeContract(contractTuple):
    newContract = Contract()
    newContract.m_symbol = contractTuple[0]
    newContract.m_secType = contractTuple[1]
    newContract.m_exchange = contractTuple[2]
    newContract.m_currency = contractTuple[3]
    newContract.m_expiry = contractTuple[4]
    newContract.m_strike = contractTuple[5]
    newContract.m_right = contractTuple[6]
    print 'Contract Values:%s,%s,%s,%s,%s,%s,%s:' % contractTuple
    return newContract

def process_Position(msg):
    contract = msg.contract
    if contract.m_secType == 'CASH':
        print contract.m_symbol + '.' + msg.contract.m_currency + '|' + str(msg.pos) + '|' + str(msg.avgCost)
    #print msg.contract['m_symbol'] + '.' + msg.contract['m_currency'] + '|' + msg.pos + '|' + msg.avgCost

def process_Historical(msg):
    if msg.date[:8] == 'finished':
        print 'historical request %d finished\n' % msg.reqId

    if msg.reqId == 11:
        hdata_1m['date'].append(msg.date)
        hdata_1m['open'].append(msg.open)
        hdata_1m['high'].append(msg.high)
        hdata_1m['low'].append(msg.low)
        hdata_1m['close'].append(msg.close)

    if msg.reqId == 12:
        hdata_2m['date'].append(msg.date)
        hdata_2m['open'].append(msg.open)
        hdata_2m['high'].append(msg.high)
        hdata_2m['low'].append(msg.low)
        hdata_2m['close'].append(msg.close)

    if msg.reqId == 13:
        hdata_5m['date'].append(msg.date)
        hdata_5m['open'].append(msg.open)
        hdata_5m['high'].append(msg.high)
        hdata_5m['low'].append(msg.low)
        hdata_5m['close'].append(msg.close)

    if msg.reqId == 14:
        hdata_15m['date'].append(msg.date)
        hdata_15m['open'].append(msg.open)
        hdata_15m['high'].append(msg.high)
        hdata_15m['low'].append(msg.low)
        hdata_15m['close'].append(msg.close)

    if msg.reqId == 15:
        hdata_30m['date'].append(msg.date)
        hdata_30m['open'].append(msg.open)
        hdata_30m['high'].append(msg.high)
        hdata_30m['low'].append(msg.low)
        hdata_30m['close'].append(msg.close)

    if msg.reqId == 16:
        hdata_1h['date'].append(msg.date)
        hdata_1h['open'].append(msg.open)
        hdata_1h['high'].append(msg.high)
        hdata_1h['low'].append(msg.low)
        hdata_1h['close'].append(msg.close)

    if msg.reqId == 17:
        hdata_1d['date'].append(msg.date)
        hdata_1d['open'].append(msg.open)
        hdata_1d['high'].append(msg.high)
        hdata_1d['low'].append(msg.low)
        hdata_1d['close'].append(msg.close)

def get_Historical(_con, _contract):
    ct = datetime.now().strftime('%Y%m%d %H:%M:%S %Z')
    whatToShow = 'MIDPOINT'
    _con.reqHistoricalData(11, _contract, ct, '2 D', '1 min', whatToShow, 0, 1)
    _con.reqHistoricalData(12, _contract, ct, '2 D', '2 mins', whatToShow, 0, 1)
    _con.reqHistoricalData(13, _contract, ct, '3 D', '5 mins', whatToShow, 0, 1)
    #_con.reqHistoricalData(14, _contract, ct, '1 W', '15 mins', whatToShow, 0, 1)
    #_con.reqHistoricalData(15, _contract, ct, '2 W', '30 mins', whatToShow, 0, 1)
    _con.reqHistoricalData(16, _contract, ct, '3 W', '1 hour', whatToShow, 0, 1)
    _con.reqHistoricalData(17, _contract, ct, '1 Y', '1 day', whatToShow, 0, 1)

def get_1m_5m(_con, _contract):
    ct = datetime.now().strftime('%Y%m%d %H:%M:%S %Z')
    whatToShow = 'MIDPOINT'
    _con.reqHistoricalData(11, _contract, ct, '2 D', '1 min', whatToShow, 0, 1)
    _con.reqHistoricalData(13, _contract, ct, '3 D', '5 mins', whatToShow, 0, 1)

if __name__ == '__main__':
    ip = '127.0.0.1'
    port = 4001
    clientId = 1
    con = ibConnection(ip, port, clientId)
    #con.registerAll(watcher)
    """
    showBidAskOnly = False# set False to see the raw messages
    if showBidAskOnly:
        con.unregister(watcher, message.tickSize, message.tickPrice,
                       message.tickString, message.tickOptionComputation)
        con.register(my_BidAsk, message.tickPrice)
    """
    con.register(fxtick, message.updateMktDepth)
    con.register(fxbidask, message.tickPrice)
    con.register(process_Position, message.position)
    con.register(process_Historical, message.historicalData)
    con.connect()

    con.reqPositions()

    f1 = open('../Data/aud_ticks.txt', 'a')
    #f2 = open('../Data/aud_bidask.txt', 'a')
    sleep(1)
    tickId = 1

    # Note: Option quotes will give an error if they aren't shown in TWS
    #contractTuple = ('ES', 'FUT', 'GLOBEX', 'USD', '200709', 0.0, '')
    contractTuple = ('AUD', 'CASH', 'IDEALPRO', 'USD', '', 0.0, '')
    fx = makeContract(contractTuple)
    #con.reqContractDetails(1, Contract)
    #con.reqAccountSummary(1, 'All')
    #con.reqCurrentTime()

    """
    Pacing Voilation
    Making identical historical data requests within 15 seconds;
    Making six or more historical data requests for the same Contract, Exchange and Tick Type within two seconds.
    Do not make more than 60 historical data requests in any ten-minute period.
    If the whatToShow parameter in reqHistoricalData() is set to BID_ASK, then this counts as two requests and we will call BID and ASK historical data separately.
    """
    #con.reqMktData(20, fx, "225", False)
    con.reqMktDepth(21, fx, 2)

    #get_Historical(con, fx)
    #con.reqRealTimeBars(10, fx, 5, 'MIDPOINT', 0)
    #schedule.every(1).minutes.at("01:01").do(job,'It is 01:00')
    #sleep(15)
    while True:
        """
        #x=datetime.today()
        #y=x.replace(day=x.day+1, hour=1, minute=0, second=0, microsecond=0)
        #delta_t=y-x
        #secs=delta_t.seconds+1
        #print secs
        #t = Timer(secs, get_Historical, con, fx)
        #t.start()
        #get_1m_5m(con, fx)
        #sleep(30)
        """
    f1.close()
    #f2.close()