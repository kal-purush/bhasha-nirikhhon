# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 12:00:08 2023

@author: Drew.Bennett


tree model to store details and run info


markBetween dialog.
    startId,
    endId,


"""





from PyQt5.QtGui import QStandardItemModel,QStandardItem
from PyQt5.QtCore import Qt

#from models.details import image_details

from image_loader import generate_details
from image_loader import load_image


import os
import csv
import json


cols = {'run':0,
        'image_id':1,
        'load':2,
        'name':3,
        'file_path':4,
        'groups':5
        }


startRole = Qt.UserRole+2
endRole = Qt.UserRole+3



class detailsTreeModel(QStandardItemModel):
    
    
    def __init__(self,parent=None):
        super().__init__(0,1,parent=parent)
        #self.setHorizontalHeaderLabels(['run','name','image id','load','file path','groups'])
        self.setHorizontalHeaderLabels([k for k in cols])


    def clear(self):
        super().clear()
        self.setHorizontalHeaderLabels([k for k in cols])
        

    #toDo: order by imageId
    def addDetail(self,run='',name='',imageId = None,load=False,filepath='',groupsString=''):
        
        if not run:
            run = generate_details.generateRun(filepath)
        
        if imageId is None:
            imageId = generate_details.generateImageId(filepath)

            
        if not name:
             name = '{run}_{imageId}'.format(run=run,imageId=imageId)
        
        if not groupsString:
            groupsString = generate_details.generateGroups(run=run,imagetype = generate_details.generateType(filepath))
        
        ri = self.runItem(run)
        if ri is None:
            ri = self._addRun(run)
        
        runIndex = self.indexFromItem(ri)
        ids = sorted(self.ids(runIndex)+[imageId])
        pos = ids.index(imageId)
        
        items = [toItem(''),toItem(imageId),toItem(load),toItem(name),toItem(filepath),toItem(groupsString)]
        ri.insertRow(pos,items)#ordered by run id
    
    
    
  #  def addDetails(self,details):
   #     for d in details:
     #       self.addDetail(d['run'],d['imageId'],filePath=d['filePath'],groupsString=d['groupsString'])
            
            
            
    def ids(self,runIndex):
        idCol = cols['image_id']
        return [self.index(r,idCol,runIndex).data() for r in range(self.rowCount(runIndex))]
      
        
    def valueRange(self,index):
        ids = self.ids(index)
        if ids:
            return (min(ids),max(ids))
        else:
            return (0,0)
        
    #remove run if no details in it.
    def _updateRun(self,runItem):
        i = self.indexFromItem(runItem)
        
        #delete if no rows in run
        if self.rowCount(i) == 0:
            self.invisibleRootItem().removeRow(runItem.row())
        
        
#raster image load file is .txt in csv format.
    def loadFile(self,file):        
        ext = os.path.splitext(file)[-1]
        if ext in ['.csv','.txt']:
          #  self.addDetails([d for d in image_details.fromCsv(file)])

            folder = os.path.dirname(file)
            
            with open(file,'r',encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                reader.fieldnames = [name.lower().replace('_','') for name in reader.fieldnames]#lower case keys/fieldnames                 
                for d in reader:
                    
                    if os.path.isabs(d['filepath']):
                        filePath = d['filepath']
                    else:
                        filePath = os.path.join(folder,d['filepath'])
                    
                    #try to convert imageId to int
                    try:
                        imageId = find(d,'imageid')
                        if imageId is not None:
                            imageId = int(imageId)
                            
                    except Exception as e:
                      #  print(e)
                        imageId = None
                    
                    
                    self.addDetail(filepath = filePath,
                                   run = find(d,'runid'),
                                   imageId = imageId,
                                   name = find(d,'name'),
                                   groupsString = find(d,'groups'))
                    


    def loadCount(self):
        total = 0
        for r in range(self.rowCount()):
            ri = self.index(r,cols['run'])
            for r2 in range(self.rowCount(ri)):
                if self.index(r2,cols['load'],ri).data() == True:
                    total+=1
        return total


    def loadImages(self,progress):
        
        progress.setMaximum(self.loadCount())
        
        total=0
        load_image.removeChild('image_loader')#remove group


        for r in range(self.rowCount()):
            ri = self.index(r,cols['run'])
            
            for r2 in range(self.rowCount(ri)):
                progress.setValue(total)
                if progress.wasCanceled():
                    return
                
                if self.index(r2,cols['load'],ri).data() == True:
                    #default groups to [] if not valid JSON
                    try:
                        groups = json.loads(self.index(r2,cols['groups'],ri).data())
                    except:
                        groups = ['image_loader']
                    
                    #loadImage(filepath,name,groups,expand=False,show=True,crs=QgsCoordinateReferenceSystem('EPSG:27700')):
                    load_image.loadImage(filepath = self.index(r2,cols['file_path'],ri).data(),
                              name = self.index(r2,cols['name'],ri).data(),
                              groups = groups)
                    total += 1


    def addFolder(self,folder):
        for i in generate_details.getImages(folder):
            #print(i)
            self.addDetail(filepath=i)


    def save(self,file):
        print('detailsTreeModel.save',file)
       

    
        with open(file,'w',newline='') as f:
            w = csv.writer(f)
            w.writerow(['filePath','runId','imageId','name','groups'])#header
            
           # q = runQuery('select file_path,run,image_id,name,groups,AsText(geom) from details',self.database())

            for r in range(self.rowCount()):
                ri = self.index(r,cols['run'])
                
                for row in range(self.rowCount(ri)):
                   
                    p = self.index(row,cols['file_path'],ri).data()
                    #can't convert to relative if on different drive
                    try:
                        p = os.path.relpath(p,file)
                    except:
                        p = self.index(row,cols['file_path'],ri).data()


                    w.writerow([p,
                            ri.data(),
                            self.index(row,cols['image_id'],ri).data(),
                            self.index(row,cols['name'],ri).data(),
                            self.index(row,cols['groups'],ri).data()
                            ])


    
    def runItem(self,run):
        
        for r in range(self.rowCount()):
            i = self.item(r,0)
            if i.data(Qt.EditRole) == run:
                return i
    
    
    #make alphabetical
    
    def _addRun(self,run):
        root = self.invisibleRootItem()
        i = QStandardItem()
        i.setData(run,Qt.EditRole)
        runs = [self.index(r,cols['run']).data() for r in range(self.rowCount())]
        root.insertRow(naturalPos(run,runs),i)
        return i
            
    
    
    def flags(self,index):
        
        if self.indexIsRun(index):
            #return Qt.ItemIsSelectable
            return super().flags(index) & ~Qt.ItemIsEditable 
        
        if index.column()==cols['run']:
            return super().flags(index) & ~Qt.ItemIsEditable 

        return super().flags(index)
    
    
        
        
    def indexIsRun(self,index):
        if index.isValid():
            return not index.parent().isValid()
        return False
     
    
    def indexIsDetail(self,index):
        return index.parent().isValid()
        
    

    def dropDetails(self,indexes):
        
        #need to remove rows in reverse order
        toRemove = {}
        for i in indexes:
            if self.indexIsDetail(i):
                runRow= i.parent().row()
                
                if runRow in toRemove:
                    toRemove[runRow].append(i.row())
                else:
                    toRemove[runRow] = [i.row()]
                
        for runRow in toRemove:
            runItem = self.itemFromIndex(self.index(runRow,0))
            for row in reversed(sorted(toRemove[runRow])):
                runItem.removeRow(row)
                self._updateRun(runItem)
        
        
      #mark all details in run  
    def markRun(self,runIndex):
        for r in range(self.rowCount(runIndex)):
            self.setData(self.index(r,cols['load'],runIndex),True)

        
      #mark all details in run  
    def unMarkRun(self,runIndex):
        for r in range(self.rowCount(runIndex)):
            self.setData(self.index(r,cols['load'],runIndex),False)
        
        
        
    def markBetween(self,runIndex,startId,endId):
        for r in range(self.rowCount(runIndex)):
            imageId = self.index(r,cols['image_id'],runIndex).data()
            if startId <= imageId and imageId <= endId:
                self.setData(self.index(r,cols['load'],runIndex),True)
        
        
        
        
def toItem(data):
    i = QStandardItem()
    i.setData(data,Qt.EditRole)
    return i


def clamp(n,minimum,maximum):
    if n < minimum:
        return minimum
    
    if n > maximum:
        return maximum
    
    return n


 #lookup value from dict, returning default if not present
def find(d,k,default=None):
    if k in d:
        return d[k]
    return default


import re

def naturalSort(l): 
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)



def naturalPos(item,items):
    s = naturalSort(items +[item])
    return s.index(item)


#runs = ['a1a','2','12','3','4','5','6','7','8','8','9','10','11a']
#print(naturalPos('6',runs))