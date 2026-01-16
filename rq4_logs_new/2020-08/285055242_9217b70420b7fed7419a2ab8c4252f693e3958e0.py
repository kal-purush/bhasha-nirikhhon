#!/usr/bin/env python3
'''
Script to delete an auction lot record
'''

import sys
import sqlite3
import time
import datetime

conn = sqlite3.connect('./Documents/foemw/foemwSource')
c = conn.cursor()

def delete_lot_data_from_input():

    # Lot ID Selection
    c.execute ('SELECT lotid, auctiondate, lotreference FROM auctionlots')
    data = c.fetchall()
    #Create list of lotid number
    lotidList = []
    for i in data:
        lotidList.append(i[0])
    for i in data:
        print(i[0],i[1],i[2])

    selectLot = int(input('Enter auction lot ID number to delete from database: '))
    if selectLot in lotidList:
        deleteLot = selectLot
    else:
        print('Wrong number!')
        sys.exit()

    # Confirmation
    print('OK to delete data from database? %s' % deleteLot)
    if str(input('y/n?')) == 'y':
        deleteFn(deleteLot)
    else: sys.exit()

def deleteFn(deleteLot):
    c.execute('DELETE FROM auctionlots WHERE lotid == ?',(deleteLot,))
    conn.commit()
    print('The database entry was deleted, here are the last 5 entries:')
    c.execute('SELECT * FROM auctionlots ORDER BY lotid DESC LIMIT 5')
    data = c.fetchall()
    for row in data:
        print(row)

delete_lot_data_from_input()

c.close()
conn.close()