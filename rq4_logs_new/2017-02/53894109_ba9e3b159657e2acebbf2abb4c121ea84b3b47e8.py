#! /usr/bin/env python

import sys
import os
import math
import getopt
import warnings
import numpy
from collections import Counter 
from collections import defaultdict
from scipy import stats
from scipy.cluster.vq import vq, kmeans2

def usage():
    print """
    quantificationEvaluator -t <truth-tsv>  -i <input-tsv>
    
    Requested Parameters:
        -t/--truth-tsv         [ string                         path to truth tsv                      ]
        -i/--input-tsv         [ string                         path to input tsv                      ]
    
    Optional Parameters:
        -s/--stratify          [ string k:s_1,s_2,...,s_k-1:v   k: number of classes                    
                                                                s_i's are separators (value or quantile) 
                                                                Default: 1                             
                                 e.g.:                                                                  
                                      -s 1                      no stratification                       
                                      -s 2                      2 classes, separate at median           
                                      -s 3                      3 classes, separate at 33%,66%          
                                      -s 4:10,50,90             4 classes, spearete at 10%,50%,90%      
                                      -s 4:0.5,1,10,100:v       4 classes, separete at the values       
                                 Note: bases on truth-tsv                                              ]     
        -b/--binary            [ no/yes default: no             If turns on (i.e. yes), correlations   
                                                                will only be caculated based on entries
                                                                with non-zero values in the truth tsv. ]             
        -o/--output            [ string default: ./result       output prefix                          ]
 
    Version:                   1.3.0
          """


#parameters
inFile = ''                     #input tsv
inTruthFile = ''                #input truth tsv
stratify = '1'                  #range separators string
isBinary = 'no'                 #bin or not bin
output_prefix = './result'      #output prefix

def getParameters(argv):
    try:
        opts, args = getopt.getopt(argv,"ht:i:s:b:o:",["help",
                                                       "truth-tsv=",
                                                       "input-tsv=",
                                                       "stratify=",
                                                       "isBinary=",
                                                       "output="])
    except getopt.GetoptError:
        usage()
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h","--help"):
            usage()
            sys.exit(1)
        elif opt in ("-i", "--input-tsv"):
            global inFile
            inFile= arg
        elif opt in ("-t","--truth-tsv"):
            global inTruthFile
            inTruthFile = arg
        elif opt in ("-s","--stratify"):
            global stratify
            stratify = arg
        elif opt in ("-b","--binary"):
            global isBinary
            isBinary = arg
        elif opt in ("-o","--output"):
            global output_prefix
            output_prefix= arg

input_values_dic = defaultdict(lambda: 0.0)

def getInputDic():
    infile = "%s" % inFile
    f=open(infile,"r")
    while True:
        line=f.readline()
        if line=="":
            break
        elif not line.startswith("ENST"):
            continue
        else:
            tmp=line.split("\t")
            name=tmp[0]
            value=float(tmp[1][0:len(tmp[1])-1])
            input_values_dic[name]=value

def checkIsBinary():
    global isBinary
    if isBinary == 'no':
        return
    else:
        if isBinary == 'Y' or isBinary =='y' or isBinary== 'Yes' or isBinary == 'yes':
            return
        else:
            print "--binary: must be yes or no."
            exit(1)

truth_values_zero = []
input_values_zero = []

truth_values = []
input_values = []

def getBothValues():
    infile = "%s" % inTruthFile
    f=open(infile,"r")
    while True:
        line=f.readline()
        if line=="":
            break
        elif not line.startswith("ENST"):
            continue
        else:
            tmp=line.split("\t")
            name=tmp[0]
            value=float(tmp[1][0:len(tmp[1])-1])
            if isBinary != 'no' and value ==0.0:
                truth_values_zero.append(0.0)
            else:
                truth_values.append(value)
            if input_values_dic.get(name) is not None:
                if isBinary != 'no' and value ==0.0:
                    input_values_zero.append(input_values_dic[name])
                else:
                    input_values.append(input_values_dic[name])
            else:
                if isBinary != 'no' and value ==0.0:
                    input_values_zero.append(0.0)
                else:
                    input_values.append(0.0)


def percentToValues(percents):
    tmp_values=sorted(truth_values)
    tmp_res = []
    if (max(percents)<1):
        print "warning!"
        print "Do you really mean "+str(min(percents))+"% - "+str(max(percents))+"%?"
    percents.sort()
    for x in range(len(percents)):
        if percents[x]<0.0 or percents[x]>=100.0:
            print "-s: "+str(percents[x])+" out of range"
            exit(1)
        index=int(len(tmp_values)*(percents[x]/100))
        if(index+1<len(tmp_values)):
            index=index+1
        tmp_res.append(tmp_values[index])
    return tmp_res

num_class = 0
separators = []

def parseSratify():
    num=stratify.count(":")
    global num_class
    global separators
    if num==0:
        num_class=int(stratify)
    if num>0:
        tmp=stratify.split(":")
        num_class=int(tmp[0])
        if num_class==1:
            print "number of class is 1, no need to set separators"
            exit(1)
    if num_class>1:
            if num>0 and len(tmp[1].split(","))!=num_class-1:
                print "number of separators not matching number of classes"
                exit(1)
            if num==0:
                pct=[]
                for x in range(num_class):
                    if x!=0:
                        pct.append(float(x)/num_class*100)
                separators=percentToValues(pct)
            if num==1:
                tmp=stratify.split(":")
                separators=percentToValues(map(float,tmp[1].split(",")))
            if num==2:
                tmp=stratify.split(":")
                if tmp[2]!='v':
                    print "check input -s the third value after : has to be v"
                    exit(1)
                else:
                    print "You are using values instead of percent" 
                separators=map(float,tmp[1].split(","))

srangeL = []
srangeR = []

def getStratifiedRanges():
    global separators
    srangeL.append(0.0)
    srangeR.append(float('inf'))
    vs = []
    global num_class
    if num_class>1:    
        srangeL.append(0.0)
        for x in range(len(separators)):
            vs.append(float(separators[x]))
        vs.sort()
        for x in range(len(vs)):
            srangeL.append(vs[x])
            srangeR.append(vs[x])
        srangeR.append(float('inf'))

def valueToRangeId(v):
    for x in range(len(srangeL)):
        if x>0 and v >=srangeL[x] and v<srangeR[x]:
            return x 

truth_values_vec = []
input_values_vec = []

def getBothStrafiedVectors():
    for x in range(len(srangeL)):
        truth_values_vec.append([])
        input_values_vec.append([])
    for x in range(len(truth_values)):
        sid=valueToRangeId(truth_values[x])    
        truth_values_vec[0].append(truth_values[x])
        input_values_vec[0].append(input_values[x])
        if len(srangeL)>1:
            truth_values_vec[sid].append(truth_values[x])
            input_values_vec[sid].append(input_values[x])

def printBinary():
    ofstring = 'off'
    if isBinary!='no':
        ofstring = 'on'
    print "Binning zero and non-zero values: "+ofstring+"\n"

def printStratify():
    print "The stratification used is: -s "+stratify
    print "The separators are:         ",' '.join(map(str, separators))
    print

def printValues():
    for x in range(len(srangeL)):
        print "range "+str(x)+":\t"+str(srangeL[x])+" - "+str(srangeR[x])+":"
        print "Truth:\t\t",' '.join(map(str, truth_values_vec[x]))
        print "Input:\t\t",' '.join(map(str, input_values_vec[x]))
        print

def calculateCor():
    final  = "range\tspearman\tpearson\tlog_pearson"
    final2 = "range\t\t\tspearman\tpearson\t\tlog_pearson"
    for x in range(len(srangeL)):
        cor,p_value=stats.spearmanr(truth_values_vec[x],input_values_vec[x])
        pearson,pearson_pvalue=stats.pearsonr(truth_values_vec[x],input_values_vec[x])
        log_pearson,log_pearson_pvalue=stats.pearsonr(numpy.log(numpy.add(truth_values_vec[x],0.01)),numpy.log(numpy.add(input_values_vec[x],0.01)))
        if isBinary != 'no' and x<=1:
            tmp  = "\n(%s,%s)\t%s\t%s\t%s" % (srangeL[x],srangeR[x],cor,pearson,log_pearson)
            tmp2 = "\n(%.3f,%.3f)\t\t%.3f\t\t%.3f\t\t%.3f" % (srangeL[x],srangeR[x],cor,pearson,log_pearson)        
        else:
            tmp  = "\n[%s,%s)\t%s\t%s\t%s" % (srangeL[x],srangeR[x],cor,pearson,log_pearson)
            tmp2 = "\n[%.3f,%.3f)\t\t%.3f\t\t%.3f\t\t%.3f" % (srangeL[x],srangeR[x],cor,pearson,log_pearson)
        final  = final  + tmp
        final2 = final2 + tmp2
        if x==0 and len(srangeL)!=1:
            final2=final2+"\n"
    print(final2)
    print
    return(final)

def calculateFDR():
    final = "FDR:\t"
    total_zero_num=len(truth_values_zero)
    input_zero_num=0
    for x in range(len(input_values_zero)):
        if input_values_zero[x]!=0.0:
            input_zero_num=input_zero_num+1
    fdr_str = str(numpy.float32(input_zero_num)/float(total_zero_num))
    final = final + fdr_str
    print
    print final
    return final

def main(argv):
    getParameters(argv[1:])
    if inFile=='' or inTruthFile=='':
        usage()
        return 1
    getInputDic()
    getBothValues()
    parseSratify() 
    getStratifiedRanges()
    getBothStrafiedVectors()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        final = calculateCor()
        final_fdr = calculateFDR()
        with open(output_prefix+".cor.out",'w') as results:
            results.write(final)
            results.close()
        with open(output_prefix+".fdr.out",'w') as results:
            results.write(final_fdr)
            results.close()
    printBinary()
    printStratify()
    printValues()
    return(0)
    
if __name__ == '__main__':
    sys.exit(main(sys.argv))