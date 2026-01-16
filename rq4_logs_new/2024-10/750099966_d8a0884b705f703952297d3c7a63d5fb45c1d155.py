import numpy
import copy
import glob

def myPrint(text):
    #print(' '.join(map(str,text)))
    a = 1

class HSP:
    def __init__(self, line):
        self.query, self.sbjct, self.identity, self.alignment_length, self.mismatches, self.gap_openings, self.query_start, self.query_end, self.sbjct_start, self.sbjct_end, self.evalue, self.bit_score = line.rstrip('\n').split('\t')

        self.identity = float(self.identity)

        self.query_start = int(self.query_start)
        self.query_end   = int(self.query_end)
        self.sbjct_start = int(self.sbjct_start)
        self.sbjct_end   = int(self.sbjct_end)

        self.alignment_length   = int(self.alignment_length)

        if self.sbjct_start < self.sbjct_end:
            self.strand = '+'
        else:
            self.strand = '-'
            tmp = self.sbjct_start
            self.sbjct_start = self.sbjct_end
            self.sbjct_end = tmp

        self.intercept = - self.query_start + self.sbjct_start
        
        self.used = False
    def toText(self):
        return '\t'.join(map(str, [self.intercept, self.query, self.sbjct, self.identity, self.alignment_length, self.mismatches, self.gap_openings, self.query_start, self.query_end, self.sbjct_start, self.sbjct_end, self.evalue, self.bit_score]))

class HSP_Manager:
    def __init__(self,pHSP_LIST, mHSP_LIST):
        self.pHSP_LIST = sorted(pHSP_LIST, key = lambda hsp : hsp.intercept)
        self.pHSP_DICT = {pHSP.intercept:pHSP_idx for pHSP_idx, pHSP in enumerate(self.pHSP_LIST)}

        self.mHSP_LIST = sorted(mHSP_LIST, key = lambda hsp : hsp.intercept)
        self.mHSP_DICT = {mHSP.intercept:mHSP_idx for mHSP_idx, mHSP in enumerate(self.mHSP_LIST)}

    def find_lHSP(self, hsp):
        hspIDX, hsp_LIST = self.find_hspIDX(hsp)
        while hspIDX - 1 >= 0:
            if hsp_LIST[hspIDX - 1].used == False:
                return hsp_LIST[hspIDX - 1]
            hspIDX += -1
            
        return None

    def find_rHSP(self, hsp):
        hspIDX, hsp_LIST = self.find_hspIDX(hsp)
        while hspIDX + 1 <= len(hsp_LIST) - 1:
            if hsp_LIST[hspIDX + 1].used == False:
                return hsp_LIST[hspIDX + 1]
            hspIDX += 1

        return None
    
    def find_hspIDX(self, hsp):
        if hsp.strand == '+':
            return self.pHSP_DICT[hsp.intercept], self.pHSP_LIST
        else:
            return self.mHSP_DICT[hsp.intercept], self.mHSP_LIST

class Group:
    def __init__(self, length):
        self.length = length

        self.hsp_LIST = []

        self.queryPos_DICT = {}
        self.sbjctPos_DICT = {}

        self.score = 0

        self.query = None
        self.sbcjt = None

    def add(self, hsp):
        myPrint([hsp.toText()])
        hsp.used = True

        self.query = hsp.query
        self.sbjct = hsp.sbjct

        self.hsp_LIST += [hsp]
        self.hsp_LIST.sort(key = lambda hsp : hsp.intercept)

        for pos in range(hsp.query_start, hsp.query_end + 1):
            self.queryPos_DICT[pos] = 1
        
        for pos in range(hsp.sbjct_start, hsp.sbjct_end + 1):
            self.sbjctPos_DICT[pos] = 1
        
        myPrint([len(self.hsp_LIST), hsp.toText()])

        tLength = 0
        mLength = 0
        for hsp in self.hsp_LIST:
            tLength += hsp.alignment_length
            mLength += hsp.alignment_length * hsp.identity

        self.mean = mLength / tLength

        queryPos_LIST = sorted(self.queryPos_DICT.keys())
        min_queryPos, max_queryPos = queryPos_LIST[0], queryPos_LIST[-1]
        queryRate = (max_queryPos - min_queryPos + 1) / self.length
        self.score = self.mean * queryRate

    def check(self, hsp):
        queryPos_DICT = copy.deepcopy(self.queryPos_DICT)
        sbjctPos_DICT = copy.deepcopy(self.sbjctPos_DICT)

        for pos in range(hsp.query_start, hsp.query_end + 1):
            queryPos_DICT[pos] = 1
        
        for pos in range(hsp.sbjct_start, hsp.sbjct_end + 1):
            sbjctPos_DICT[pos] = 1

        queryPos_LIST = sorted(queryPos_DICT.keys())
        sbjctPos_LIST = sorted(sbjctPos_DICT.keys())

        min_queryPos, max_queryPos = queryPos_LIST[0], queryPos_LIST[-1]
        min_sbjctPos, max_sbjctPos = sbjctPos_LIST[0], sbjctPos_LIST[-1]

        queryRate = (max_queryPos - min_queryPos + 1) / self.length
        sbjctRate = (max_sbjctPos - min_sbjctPos + 1) / self.length

        queryFull = len(queryPos_LIST) / (max_queryPos - min_queryPos +1)
        sbjctFull = len(sbjctPos_LIST) / (max_sbjctPos - min_sbjctPos + 1)
        myPrint([queryRate, sbjctRate, queryFull, sbjctFull])

        if queryRate > 1.1: return False
        if sbjctRate > 1.1: return False

        if queryRate / sbjctRate > 2: return False
        if sbjctRate / queryRate > 2: return False

        if queryFull < 0.8: return False
        if sbjctFull < 0.8: return False

        return True
    def toText(self):
        queryPos_LIST = sorted(self.queryPos_DICT.keys())
        sbjctPos_LIST = sorted(self.sbjctPos_DICT.keys())

        min_queryPos, max_queryPos = queryPos_LIST[0], queryPos_LIST[-1]
        min_sbjctPos, max_sbjctPos = sbjctPos_LIST[0], sbjctPos_LIST[-1]

        queryRate = (max_queryPos - min_queryPos + 1) / self.length
        sbjctRate = (max_sbjctPos - min_sbjctPos + 1) / self.length

        queryFull = len(queryPos_LIST) / (max_queryPos - min_queryPos +1)
        sbjctFull = len(sbjctPos_LIST) / (max_sbjctPos - min_sbjctPos + 1)
        return '\t'.join(map(str, ['{0:35s}'.format(self.query), self.sbjct, '{0:8d}'.format(len(self.hsp_LIST)), '{0:8d}'.format(self.length), '{0:12.4f}'.format(numpy.mean([hsp.intercept for hsp in self.hsp_LIST])), '{0:8.4f}'.format(self.score), '{0:8.4f}'.format(self.mean), '{0:8d}'.format(min_queryPos), '{0:8d}'.format(max_queryPos), '{0:8d}'.format(min_sbjctPos), '{0:8d}'.format(max_sbjctPos), '{0:8.4f}'.format(queryRate), '{0:8.4f}'.format(sbjctRate), '{0:8.4f}'.format(queryFull), '{0:8.4f}'.format(sbjctFull)]))
    def toArray(self):
        queryPos_LIST = sorted(self.queryPos_DICT.keys())
        sbjctPos_LIST = sorted(self.sbjctPos_DICT.keys())

        min_queryPos, max_queryPos = queryPos_LIST[0], queryPos_LIST[-1]
        min_sbjctPos, max_sbjctPos = sbjctPos_LIST[0], sbjctPos_LIST[-1]

        queryRate = (max_queryPos - min_queryPos + 1) / self.length
        sbjctRate = (max_sbjctPos - min_sbjctPos + 1) / self.length

        queryFull = len(queryPos_LIST) / (max_queryPos - min_queryPos +1)
        sbjctFull = len(sbjctPos_LIST) / (max_sbjctPos - min_sbjctPos + 1)
        return [self.query, self.sbjct, len(self.hsp_LIST), self.length, numpy.mean([hsp.intercept for hsp in self.hsp_LIST]), self.score, self.mean, min_queryPos, max_queryPos, min_sbjctPos, max_sbjctPos, queryRate, sbjctRate, queryFull, sbjctFull]

def Find_bestMatch(fileName, length):
    aHSP_LIST = []
    pHSP_LIST = []
    mHSP_LIST = []

    fin = open(fileName)
    targetID = None
    for line in fin:
        hsp = HSP(line)
        if targetID == None:
            targetID = hsp.sbjct
        elif targetID != hsp.sbjct: continue

        if hsp.identity < 95: continue

        #if hsp.alignment_length < 100: continue

        if hsp.strand == '+':
            pHSP_LIST += [hsp]
        else:
            mHSP_LIST += [hsp]
        
        aHSP_LIST += [hsp]

    myPrint(['aHSP_LIST', len(aHSP_LIST)])
    myPrint(['pHSP_LIST', len(pHSP_LIST)])
    myPrint(['mHSP_LIST', len(mHSP_LIST)])

    hsp_Manager = HSP_Manager(pHSP_LIST, mHSP_LIST)


    group_LIST = []

    for hsp in aHSP_LIST:
        if len(group_LIST) > 5: break

        if hsp.used == True: continue

        #group = Group(30241)
        group = Group(length)
        myPrint(['new!!!    ', hsp.toText() + '\n'])
        group.add(hsp)
        group_LIST += [group]

        
        while True:
            myPrint(['Find..   '  + group.hsp_LIST[0].toText()])
            myPrint(['Find..   '  + group.hsp_LIST[-1].toText()])
            lHSP = hsp_Manager.find_lHSP(group.hsp_LIST[0])
            rHSP = hsp_Manager.find_rHSP(group.hsp_LIST[-1])

            if lHSP == None:
                myPrint(['Left...   ' + 'None'])
            else:
                myPrint(['Left...   ' + lHSP.toText()])
            
            if rHSP == None:
                myPrint(['Right..   ' + 'None'])
            else:
                myPrint(['Right..   ' + rHSP.toText()])

            if lHSP == None and rHSP == None: break
            elif lHSP == None: tHSP = rHSP
            elif rHSP == None: tHSP = lHSP
            elif group.hsp_LIST[0].intercept - lHSP.intercept < rHSP.intercept - group.hsp_LIST[-1].intercept:
                myPrint(['chosed Left:  ', lHSP.toText()])
                tHSP = lHSP
            else:
                myPrint(['chosed Right: ', rHSP.toText()])
                tHSP = rHSP

            if group.check(tHSP) == True:
                myPrint(['add!\n'])
                group.add(tHSP)
            else:
                break
            
    group_LIST.sort(key = lambda group : group.score, reverse=True)


    result_LIST = []
    for group in group_LIST[0:5]:
        result_LIST += [group.toArray()]
        #print(group.toText())
        #myPrint([group.toText()])
    return result_LIST

from joblib import Parallel, delayed

def run_batch(fileName_LIST, batchN):
    def run_single(fileName):
        fin = open(fileName)
        data_LIST = fin.readline().split('\t')

        query = fileName.split('/')[-1].split('.')[0]
        result_LIST = []
        if len(data_LIST) == 1:
            pass
        else:
            start, end = map(int, data_LIST[0].split(':')[1].split('-'))
            length = end - start + 1

            result_LIST = Find_bestMatch(fileName, length)
        
        fout = open('result/' + query + '.bestMatch', 'w')
        for result in result_LIST:
            fout.write('\t'.join(map(str, result)) + '\n')
        fout.close()


    Parallel(n_jobs=batchN)(delayed(run_single)(fileName) for fileName in fileName_LIST)

fileName_LIST = glob.glob('../01.blastn/result/*.blastn')
fileName_LIST.sort()

print(len(fileName_LIST))

run_batch(fileName_LIST, 128)