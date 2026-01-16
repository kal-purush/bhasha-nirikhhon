# -*- coding: utf8 -*-
import time
from PyQt4 import QtCore
from PyQt4.QtGui import *
from Core_Pathologie import *
from Core_Critere import *

from DBase import Request
from MyGenerics import *

DATE,idCONSULTANT,idTYPECONSULTATION,idREFERANT,EXAMEN,TRAITEMENT,COMMENTAIRE,isACTIF,isDELETED,TYPECONSULTATION,CONSULTANT,REFERANT,isBIOLOGIE,isIMAGE,isCHIRURGIE,isORDONNANCE,isPLANTHERAPEUTIQUE = range(2,19)

class Vaccination(MyModel):
    def __init__(self,idAnimal,idTable=0,parent=None,*args):
        self.parent=parent
        self.MyRequest=Request(parent)
        self.idAnimal=idAnimal
        self.idVaccination=idTable
        MyModel.__init__(self,'Vaccin',idTable,parent,*args)
        if idTable==0:
            self.MyLots=MyComboModel(parent)
        else:
            self.MyLots=MyComboModel(parent,'GetVaccinationLots(%i)'%idTable)
        if idTable==0:
            self.MyRappels=MyComboModel(parent)
        else:
            self.MyRappels=MyComboModel(parent,'GetRappelVaccination(%i)'%idTable)   
            
 #       print self.listdata   
#        self.listdata=self.MyRequest.GetLineTable(self.idVaccination)

    def SetVaccin(self,idTable):
        if idTable==0:
            self.New()
        else:
            self.listdata=self.MyRequest.GetLineTable(idTable)
        self.MyLots.Set('GetVaccinationLots(%i)'%idTable)
        self.MyRappels.Set('GetRappelVaccination(%i)'%idTable)
        
    def GetNewVaccin(self):
        return self.MyRequest.GetInt("CALL GetNewVaccin(%i)"%self.idAnimal)

    def GetLot(self,idValence):
        return self.MyRequest.GetInt("CALL GetLots(%i)"%idValence)

class Lot():
    def __init__(self,idLot,parent):
        self.idLot=idLot
        self.MyRequest=Request(parent)
        self.listdata=self.MyRequest.GetLine("CALL GetLot(%i)"%self.idLot)
        
    def GetData(self,col):
        if col==4:
            return self.listdata[col].toDate()      #.toString('dd/MM/yyyy')
        if col in [5,6]:
            return self.listdata[col].toInt()[0]
        else:
            return self.listdata[col].toString()
        
    def GetNewLine(self):
        if self.GetData(6)>0:
            rq=u'Périmé ou vide! %s'%self.GetData(4).toString('dd/MM/yyyy')
        else:
            rq=self.GetData(4).toString('dd/MM/yyyy')
        return([self.idLot,'%s : %s'%(self.GetData(2),self.GetData(3)),QString(rq),self.GetData(6),0])
 
        
class FormLot(MyForm):   
    def __init__(self,data,parent):
        MyForm.__init__(self,u'Edition des Valences vaccinales',data,parent)
        self.parent=parent
        self.InactivateEnter()
        self.fields[0].setModel(MyComboModel(self.parent,'GetFournisseursVaccin()'))            
        self.EditButtons[0].clicked.connect(self.OnEditFournisseur)
        
    def OnEditFournisseur(self):
        idRelance=self.fields[0].model().data(self.fields[0].currentIndex(),Qt.UserRole).toInt()
        if idRelance[1]:
            model=MyModel('Fournisseur',idRelance[0],self)
            #TODO:
            self.SetModel(model,{1:3,2:2,3:4,4:5,5:6,6:7,7:8,9:10})
           
 