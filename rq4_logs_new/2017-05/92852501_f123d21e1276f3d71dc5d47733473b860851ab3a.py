#############################################################################################################
#
#       Date        |       Author          |       Description
#   ----------------|-----------------------|---------------------------------------------------------------
#   Mar 24, 2017    |   Anurag Dixit        |   Adding functionalities to parse data across multiple files.
#   May 9, 2017     |   Pavan Joshi         |   Changing parsing functionalities to make the data suitable
#                   |                       |   for Markov Model
#   May 10, 2017    |   Pavan Joshi         |   Re-mapping indices to its proper columns
#   May 10, 2017    |   Pavan Joshi         |   Added functionalities to create bins given data and
#                   |                       |   to get bins given data
#   May 11, 2017    |   Pavan Joshi         |   Implemented functionalities to bin the data and to get factors
#   ----------------|-----------------------|---------------------------------------------------------------
#
#############################################################################################################

import os
import csv
import numpy as np

class DataProcessor(object):

    def __init__(self,path):
        self.initIndices()
        print("===============================================================")
        self.readCSVFiles(path)
        print("===============================================================")
        self.parseData()
        print("===============================================================")
        self.binData()
        print("===============================================================")

    def initIndices(self):
        self.idx = dict()
        self.idx['pagePopularity'] = 0
        self.idx['pageCheckins'] = 1
        self.idx['pageTalkingAbt'] = 2
        self.idx['pageCategory'] = 3
        self.idx['cc1'] = 29
        self.idx['cc2'] = 30
        self.idx['cc3'] = 31
        self.idx['cc4'] = 32
        self.idx['cc5'] = 33
        self.idx['baseTime'] = 34
        self.idx['postLength'] = 35
        self.idx['postShareCt'] = 36
        self.idx['postPromotion'] = 37
        self.idx['postDay'] = range(39,46)
        self.idx['baseDay'] = range(46,53)
        self.idx['Comments'] = 53

        self.discrete = ['pageCategory','postDay','baseDay','postPromotion']

        self.bin = dict()
        self.bin['pagePopularity'] = 200
        self.bin['pageCheckins'] = 200
        self.bin['pageTalkingAbt'] = 200
        self.bin['cc1'] = 200
        self.bin['cc2'] = 200
        self.bin['cc3'] = 200
        self.bin['cc4'] = 200
        self.bin['cc5'] = 200
        self.bin['baseTime'] = 71
        self.bin['postLength'] = 300
        self.bin['postShareCt'] = 200
        self.bin['Comments'] = 333

        self.metadata = dict()

    def readCSVFiles(self,path):
        print('Reading CSV Files from: '+path)
        self.data = []
        csvFiles = [os.path.join(path,fName) for fName in os.listdir(path) if fName.endswith(".csv")]
        for csvFile in csvFiles:
            with open(csvFile) as csvFileHandle:
                reader = csv.reader(csvFileHandle, delimiter=',',quoting=csv.QUOTE_NONE)
                for row in reader:
                    self.data.append(row)
        self.data = np.array(self.data,dtype=np.float32)
        self.data = self.data[self.data[:,self.idx['Comments']]<333]
        self.data = self.data[self.data[:,self.idx['Comments']]!=333]

    def reduceDimension(self,idx, reduction_type):
        if reduction_type == 'day':
            truncateIdx = list(set(range(54))-set(idx))
            temp = np.delete(self.data,truncateIdx,1)
            self.data[:,idx[0]] = np.array(np.argmax(temp,
    							axis=1).reshape(temp.shape[0],),
    							dtype=np.int32)
            indices = idx[0]
        return indices

    def parseData(self):
        print('Parsing Data & Performing Dimensionality Reduction')
        self.idx['postDay'] = self.reduceDimension(self.idx['postDay'],'day')
        self.idx['baseDay'] = self.reduceDimension(self.idx['baseDay'],'day')
        indices = np.hstack(self.idx.values())
        truncateIdx = list(set(range(self.data.shape[1])) - set(indices))
        self.data = np.delete(self.data,truncateIdx,axis=1)
        self.data = np.array(self.data,dtype=np.int32)
        count = 0
        for key in sorted(self.idx,key=self.idx.get):
            self.idx[key] = count
            count += 1

    def createBins(self,column,bins=6):
        idx = self.idx[column]
        data = self.data[:,idx]
        minimum = np.min(data)
        maximum = np.max(data)
	mean = np.mean(data)
        step = np.round((maximum - minimum)/np.float(bins))
        bins = np.arange(minimum,maximum,step)
        metadata = dict()
        metadata['max'] = maximum
        metadata['min'] = minimum
        metadata['nBins'] = bins.shape[0]
        metadata['step'] = step
        metadata['bins'] = bins
	metadata['mean'] = mean
        self.metadata[column] = metadata

    def getBin(self,value,column):
        if column in self.discrete:
            return value
        metadata = self.metadata[column]
        if value == metadata['max']:
            return metadata['nBins'] - 1
        return np.floor((value - metadata['min'])/metadata['step'])

    def binData(self):
        print('Performing Descretization')
        for key in self.idx.keys():
            if key not in self.discrete:
                idx = self.idx[key]
                data = self.data[:,idx]
                bins = self.bin[key]
                self.createBins(key,bins)
                metadata = self.metadata[key]
                bins = metadata['bins']
                for i in range(len(bins)):
                    if i+1==len(bins):
                        temp = (data > bins[i])
                    else:
                        temp = np.logical_and(data > bins[i],
                                    data < bins[i+1])
                    data[temp] = i


                self.data[:,idx] = data

    def getFactors(self,column_one,column_two=None):
        idx_one = self.idx[column_one]
        if column_two != None:
            idx_two = self.idx[column_two]
            truncateIdx = list(set(self.idx.values()) - set([idx_one,idx_two]))
            data = np.delete(self.data,truncateIdx,axis=1)
            if idx_one > idx_two:
                idx_one = 1
                idx_two = 0
            else:
                idx_one = 0
                idx_two = 1
            cardinality = (np.unique(data[:,idx_one]).shape[0],
                            np.unique(data[:,idx_two]).shape[0])
            factors = np.zeros(cardinality,dtype=np.float32)
            m = float(data.shape[0])
            for i in range(cardinality[0]):
                temp = data[data[:,idx_one]==i]
                for j in range(cardinality[1]):
                    factors[i,j] = temp[temp[:,idx_two]==j].shape[0]/m
        else:
            truncateIdx = list(set(self.idx.values()) - set([idx_one]))
            data = np.delete(self.data,truncateIdx,axis=1)
            cardinality = [np.unique(data[:,0]).shape[0]]
            factors = np.zeros(cardinality,dtype=np.float32)
            m = float(data.shape[0])
            for i in range(cardinality[0]):
                temp = data[data[:,0]==i]
                factors[i] = temp.shape[0]/m
                #print i," : ",np.unique(data[:,0])[i]," : ",temp.shape[0]," : ",factors[i]

        retVal = dict()
        retVal['factors'] = factors
        retVal['cardinality'] = cardinality

        return retVal


if __name__ == "__main__":
    directoryPath = os.path.abspath("Training/")
    DataProcessor(directoryPath)