# -*- coding: utf-8 -*-
"""
Created on Thu Jun 07 16:14:08 2018

@author: Fatemeh
"""
import sys
sys.path.append(r"C:\Users\Fatemeh\Desktop\warehouseSystem\Model")
from pyodbc import *
from Warehouse import Warehouse
from Commodity import Commodity
import pandas as pd

class CommodityAction(object):
    def addCommodity(self,Commodity, Warehouse):
        SQLCommand =  ("INSERT INTO Commodity"
                      "(CommodityName ,Type ,number ,price , WarehouseName)"
                      " VALUES ('%s','%s','%d','%s','%s')"%(Commodity.name ,Commodity.Type ,Commodity.number ,Commodity.price,Warehouse.name))
        
        
        
    def reduceCommodity(self,Commodity,numOfLost,Warehouse):
        number = self.getNumber(Commodity) - numOfLost
        SQLCommand = ("UPDATE Commodity SET number=%d WHERE CommodityName=%s,Type=%s,number=%d,price=%s,WarehouseName=%s" 
                      %(number,self.getName(Commodity) ,self.getType(Commodity),self.getNumber(Commodity) ,self.getPrice(Commodity) ,Warehouse.name))
        
    def delCommodity(self,Commodity, Warehouse):
        SQLCommand = ("DELETE FROM Commodity WHERE "
                      "(CommodityName ,Type ,number ,price , WarehouseName)"
                      " VALUES ('%s','%s','%d','%s','%s')"%(self.getName(Commodity) ,self.getType(Commodity)
                               ,self.getNumber(Commodity) ,self.getPrice(Commodity) ,Warehouse.name))
        
    def editCommodity(self, Commodity, newName, newType, newNumber, newPrice, Warehouse):
         SQLCommand = ("UPDATE Commodity SET CommodityName=%s ,Type=%s ,number=%d ,price=%s WHERE CommodityName=%s ,Type=%s ,number=%d ,price=%s "
                       %(newName, newType, newNumber, newPrice,self.getName(Commodity) ,self.getType(Commodity)
                        ,self.getNumber(Commodity) ,self.getPrice(Commodity) ,Warehouse.name))
                      
                      
    def getTotalNumOfCommodity(self,Warehouse):
        Total = pd.read_sql("SELECT SUM(number) FROM Commodity WHERE WarehouseName='%s' "
                            %(Warehouse.name))
        return Total
        
        
    """def checkEmptyOfCapacity(self,Warehouse):
        if Warehouse.capacity == 0.0 :
            return "capacity of "
    """ 
        
        
    def showAllCommodity(self,Warehouse):
        showCommodities = pd.read_sql("SELECT CommodityName FROM Commodity WHERE WarehouseName='%s' "
                                      %(Warehouse.name))
        return showCommodities
        
    def checkNumOfCommodity(self,Commodity):
        numOfCommodity = pd.read_sql("SELECT number FROM Commodity WHERE (CommodityName ,Type ,price)"
                                     "VALUES('%s','%s','%s')"
                                     %(Commodity.name,Commodity.Type, Commodity.price))
        return numOfCommodity
    
    def showSpecificationCommodity(self, Commodity):
        showSpecification = pd.read_sql("SELECT * FROM Commodity WHERE WarehouseName='%s' "
                                        %(Warehouse.name))
        return showSpecification
        

    def getName(self,Commodity):
        return Commodity.name        
        
    def getType(self,Commodity):
        return Commodity.Type
    
    def getNumber(self,Commodity):
        return Commodity.number
    
    def getPrice(self,Commodity):
        return Commodity.price
    
                
        
        
        
        
        
        
        
        
        