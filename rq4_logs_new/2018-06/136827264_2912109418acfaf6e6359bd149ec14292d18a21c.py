# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 00:16:17 2018

@author: admin
"""

import sys
#sys.path.append(r"C:\Users\Fatemeh\Desktop\warehouseSystem\Model")
import pandas as pd
from Request import Request
from Company import Company
from Commodity import Commodity
from CommodityAction import CommodityAction

class RequestAction(object):
    def requestCommodity(self ,Request,Commodity ,Company):
            SQLCommand = ("INSERT INTO Request"
                      "(RequestNumber, date, status, CommodityName ,Type ,number ,price ,ComapanyName)"
                      " VALUES ('%d','%s','%s','%s','%s','%d','%s','%s')"
                      %(self.getRequestNumber(Request),self.getDate(Request),'2',
                        Commodity.name,Commodity.Type,Commodity.number,Commodity.price, Company.name))
    
#    def numberOfRequest(self, Commodity, Warehouse ,Company):
#        RequestNum = pd.read_sql("SELECT number FROM Request WHERE (CommodityName,CompanyName,WarehouseName)"
#                               "VALUES('%s','%s','%s')"%(Commodity.name,Company.name,Warehouse.name))
#        return RequestNum
    
    def checkStatusRequest(self,Request):
        status = pd.read_sql("SELECT status FROM Warehouse WHERE"
                     "(requestID, date)"
                     "VALUES ('%d','%s')"%
                     (self.getOrderNumber(Request),self.getDate(Request)))
        return status
        
    def update(self,Commodity,Request,Company):
        numOfLost = self.getRequestNumber(Commodity, Warehouse, Request.asker)
        CommodityAction.reduceCommodity(self,Commodity,numOfLost,Warehouse)
            SQLCommand = ("UPDATE Warehouse SET WarehouseName=%s,Capacity=%f WHERE WarehouseName=%s, Capacity=%f"
                     %(newName,newCapacity,Warehouse.name,Warehouse.capacity))
        
        
    def getRequestNumber(self,Request):
        return Request.number
    
    def getRequestDate(self,Request):
        return Request.date
    
    def getRequestStatus(self,Request):
        return Request.status