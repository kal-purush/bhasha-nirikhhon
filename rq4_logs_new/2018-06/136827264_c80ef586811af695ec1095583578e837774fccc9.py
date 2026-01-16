# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 00:10:05 2018

@author: admin
"""

import sys
sys.path.append(r"C:\Users\Fatemeh\Desktop\warehouseSystem\Model")
from pyodbc import *
from Warehouse import Warehouse
from Company import Company


class CompanyAction(object):


    def createCompany(self,Company):
        SQLCommand = ("INSERT INTO Company "
                 "(ComapanyName, Email,phone,address)"
                 "VALUES ('%s','%s','%d','%s')"%
                         (Company.name,Company.email,Company.phone,Company.address))



    def connectDB(self):
        self.connection = connect(r'Driver={SQL Server};Server=DESKTOP-6QFOIGQ\FATEMEH;Database=WarehouseSystem;Trusted_Connection=yes;')
        self.cursor = self.connection.cursor()