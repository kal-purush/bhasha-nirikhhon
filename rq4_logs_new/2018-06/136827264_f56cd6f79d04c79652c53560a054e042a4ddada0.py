# -*- coding: utf-8 -*-
"""
Created on Sun Jun 1 11:34:16 2018

@author: R.S
"""

import sys
sys.path.append(r"F:\B.Sc\Term 6\SE\Project\warehouse-phaze_1\WarehouseSystem\model")
from Commodity import Commodity
from Settings import Setting
import pandas as pd
from Request import Request
from Company import Company
from Commodity import Commodity
from Warehouse import Warehouse
from CommodityAction import CommodityAction

class RequestAction(object):
    def addRequest(self,Request ,Commodity ,Warehouse,Company):
        
          SQLCommand = ("INSERT INTO Request"
                        "(requestID, date, status, CommodityName ,Type ,number ,price ,ComapanyName,WarehouseName)"
                        " VALUES ('%d','%s','%s','%s','%s','%d','%s','%s')"
                        %(self.getRequestID(Request),self.getDate(Request),'2',
                        Commodity.name,Commodity.Type,Commodity.number,Commodity.price, Company.name,Warehouse.name))
             
                                   
   
    def getRequestID(self,Request):
        return Request.requestID
    
    def getDate(self,Request):
        return Request.date
    
    def getStatus(self,Request):
        return Request.status
    
                     
                        



         
    def numberOfRequest(self, Commodity, Warehouse ,Company):
        requestNum = pd.read_sql("SELECT number FROM Request WHERE (CommodityName,CompanyName,WarehouseName)"
                                 "VALUES('%s','%s','%s')"%(Commodity.name,Company.name,Warehouse.name))
        return requestNum
    
    def checkStatusRequest(self,Request):
        status = pd.read_sql("SELECT status FROM Warehouse WHERE"
                             "(requestID, date)"
                             "VALUES ('%d','%s')"%
                             (self.getRequestID(Request),self.getDate(Request)))
        return status
        
    def update(self,Commodity,Request,Warehouse):
        numOfLost = self.getRequestID(Commodity, Warehouse, Request.asker)
        CommodityAction.reduceCommodity(self,Commodity,numOfLost,Warehouse)
            SQLCommand = ("UPDATE Warehouse SET WarehouseName=%s,Capacity=%f WHERE WarehouseName=%s, Capacity=%f"
                     %(newName,newCapacity,Warehouse.name,Warehouse.capacity))
        
        
        
                              
                              
                              
                              
  