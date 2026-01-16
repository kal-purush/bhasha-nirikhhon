# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 09:29:35 2020
@email ：appie777ny@gmail.com
@author: WPP
"""

import requests,math,os,threading,argparse
import pandas as pd
import numpy as np
from queue import Queue
from argparse import RawTextHelpFormatter

parser = argparse.ArgumentParser(description="""Searching the reference PMID and cited PMCID,if necessary,change the cited PMCID to PMID\n\
output filenames                    : reference_cited_PMID_Result.txt\n\
PMCID-PMID converting INFO fileName : PMCID-PMID Converting Data.txt\n""",formatter_class=RawTextHelpFormatter)

parser.add_argument('-i',   help='inputFile directory,"csv" or "txt" types.')
parser.add_argument('-coln',default='pubmed_id',                            help="column name of pmid in inputFile,        default: pubmed_id")
parser.add_argument('-api', default='3d20b15ed393b63857232da6279e8cd94708', help="APIkey, may not be useful in the future  default: 3d20b15ed393b63857232da6279e8cd94708")
parser.add_argument('-t',   default='my_tool',                              help="tool,                                    default: my_tool")
parser.add_argument('-e',   default='my_email@example.com',                 help="email,                                   default: my_email@example.com")
parser.add_argument('-cv',  default='True',                                 help="weather convert the cited PMCID to PMID, default: True , if don't need, set 'False'")

args = parser.parse_args()

inputFilePath           = args.i
idListName              = args.coln
apiKey                  = args.api
tool                    = args.t
email                   = args.e
weatherConvertPMCtoPM   = args.cv

### note that the nan value wwill be replaced by the string character 'None' to avoid some error when reading or writing file with pandas.
if not os.path.exists('Result'):
    os.makedirs('Result')
elink = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?"
# --------------- get reference PMID ---------------

def getReferencePmid(pmidList,apiKey):

    referenceSearchResult = []
    # the post request to get reference PMID ,  no more than 5000 pmid in one post request are needed.
    for i in range(math.ceil(len(pmidList)/5000)):
        print('Searching the reference PMID')
        print('%d PMID are being searched\n'%len(pmidList))
        reference_postData = {
                            'dbfrom'    : 'pubmed',
                            'linkname'  : 'pubmed_pubmed_refs',
                            'id'        : pmidList[5000*i:5000*(i+1)],
                            'api_key'   : apiKey,
                            'retmode'   : 'json',
                            }
        
        # timeout was set to 30 seconds
        condition = 0
        while condition ==0:
            try:
                referenceSearchResult = referenceSearchResult + eval(requests.post(elink,data=reference_postData,timeout=30).text)['linksets']
                condition = 1
            except:
                print('Request timed out Error')
    return referenceSearchResult

# --------------- get cited PMCID ---------------

def getCitedPmid(pmidList,apiKey):
    print('Searching the cited PMCID') 
    print('%d PMID are being searched\n'%len(pmidList))
    citedSearchResult = []
    # the post request to get cited PMCID , no more than 10000 pmid in one post request are needed.
    for i in range(math.ceil(len(pmidList)/10000)):
        cited_postData = {
                        'dbfrom'    : 'pubmed',
                        'linkname'  : 'pubmed_pmc_refs',
                        'id'        : pmidList[10000*i:10000*(i+1)],
                        'api_key'   : apiKey,
                        'retmode'   : 'json',
                        }
    
        # timeout 30s
        condition = 0
        while condition ==0:
            try:
                citedSearchResult = citedSearchResult + eval(requests.post(elink,data=cited_postData,timeout=30).text)['linksets']
                condition = 1
            except:
                print('Request timed out Error')
    return citedSearchResult



# pmcid to Pmid
# --------------- pmcid convert to pmid  update on the history data , note that only recode pmcid and pmid---------------
# no more than 200 idList in one post are needed,  tool=my_tool, email=my_email@example.com
def searchPMCID_PMID_convertData(pmcIDList,apiKey,tool,email,backValue):
    pmcidToPmidConverterLink = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?'
    pmcidToPmidConverter_postData = {
                                    'ids'       : ','.join(pmcIDList),
                                    'idtype'    : 'pmcid',
                                    'api_key'   : apiKey,
                                    'tool'      : tool,
                                    'email'     : email,
                                    'format'    : 'json',
                                    'versions'  : 'no',
                                    }
    # timeout 5s 
    condition = 0
    tryTime = 0
    while condition == 0:
        tryTime += 1
        if tryTime>5:
            print('Net error or IP forbidden, check the Internet or the account API')
            break
        try:
            pmcidToPmidConverterResult = eval(requests.post(pmcidToPmidConverterLink,data=pmcidToPmidConverter_postData,timeout=5).text)['records']
            condition = 1
        except:
            print('Request timed out Error')
    backValue.put(pmcidToPmidConverterResult)

# update PMCID-PMID Converting Data.csv
def multiTaskConvertPMCtoPM(newPmcIDList,apiKey,tool,email):

    if os.path.exists('Result\\pmcid to pmid.csv'):
        hisPmcidToPmidConverterResultData = pd.read_csv('Result\\pmcid to pmid.csv',index_col=None,dtype=str)
        his_PMCID = [x for x in list(hisPmcidToPmidConverterResultData['pmcid'])]
    else:
        hisPmcidToPmidConverterResultData = pd.DataFrame()
        his_PMCID = []
    searchPmcidList = [x for x in newPmcIDList if str(x) not in his_PMCID]
    if searchPmcidList==[]:
        return []
    backValue = Queue()
    pmcidToPmidConverterResult = []
    if len(searchPmcidList)<200:
        searchPMCID_PMID_convertData(searchPmcidList,apiKey,tool,email)
    else:
        while searchPmcidList!=[]:
            print('%d PMCID needed to be searched'%(len(searchPmcidList)))
            thrSearchPmcidList = searchPmcidList[:1000]
            threadNum = math.ceil(len(thrSearchPmcidList)/200)
            TASK = []
            for i in range(threadNum):           
                TASK.append(threading.Thread(target=searchPMCID_PMID_convertData,args=(thrSearchPmcidList[i*200:(i+1)*200],apiKey,tool,email,backValue)))
            for task in TASK:
                task.start() 
            for task in TASK:
                task.join()
            for i in range(len(TASK)):
                pmcidToPmidConverterResult = pmcidToPmidConverterResult + backValue.get()
            searchPmcidList = [x for x in searchPmcidList if x not in thrSearchPmcidList]
    
    # update 
    pmcidToPmidConverterResult = [x for x in pmcidToPmidConverterResult if 'pmid' in x.keys()]
    if pmcidToPmidConverterResult!=[]:
        pmcidToPmidConverterResult_dict = dict(zip(range(len(pmcidToPmidConverterResult)),pmcidToPmidConverterResult))
        newPmcidToPmidConverterResultData = pd.DataFrame.from_dict(pmcidToPmidConverterResult_dict,'index')[['pmcid','pmid']]
        newPmcidToPmidConverterResultData['pmcid'] = [x[3:] for x in newPmcidToPmidConverterResultData['pmcid']]
        pmcidToPmidConverterResultData = pd.concat( [hisPmcidToPmidConverterResultData,newPmcidToPmidConverterResultData], axis=0 )
        pmcidToPmidConverterResultData.to_csv('Result\\pmcid to pmid.csv',index=None)
    else:
        print('No PMCID to PMID update')

def convertToNetFile():
    outputData = pd.read_table('Result\\reference_cited_PMID_Result.txt',index_col=None,dtype=str)
    
    # Pmid to reference pmid
    if os.path.exists('Result\\pmid to reference pmid.csv'):
        hisPmidToReferencePmid = pd.read_csv('Result\\pmid to reference pmid.csv',index_col=None,dtype=str)
        hisPMID = list(set(hisPmidToReferencePmid['pmid']))
    else:
        hisPmidToReferencePmid = pd.DataFrame(columns = ['pmid' , 'reference pmid'])
        hisPMID = []
    
    newPmidToReferencePmid = pd.DataFrame(columns = ['pmid' , 'reference pmid'])
    pmidWithReference = outputData[outputData['reference pmid']!='None'].copy()
    pmidWithReference.index = list(pmidWithReference['pmid'])
    netData = [[x,y] for x in list(pmidWithReference['pmid'])  for y in pmidWithReference['reference pmid'][x].split(',') if x not in hisPMID]
    newPmidToReferencePmid['pmid'] = list(hisPmidToReferencePmid['pmid']) + [x[0] for x in netData]
    newPmidToReferencePmid['reference pmid'] = list(hisPmidToReferencePmid['reference pmid']) + [x[1] for x in netData]
    newPmidToReferencePmid.to_csv('Result\\pmid to reference pmid.csv',index=None)
    
    # Pmid to cited pmcid
    if os.path.exists('Result\\pmid to cited pmcid.csv'):
        hisPmidToCitedPmcid = pd.read_csv('Result\\pmid to cited pmcid.csv',index_col=None,dtype=str)
        hisPMID = list(set(hisPmidToCitedPmcid['pmid']))
    else:
        hisPmidToCitedPmcid = pd.DataFrame(columns = ['pmid' , 'cited pmcid'])
        hisPMID = []
    newPmidToCitedPmcid = pd.DataFrame(columns = ['pmid' , 'cited pmcid'])
    pmidWithCited = outputData[outputData['cited pmcid']!='None'].copy()
    pmidWithCited.index = list(pmidWithCited['pmid'])
    netData = [[x,y] for x in list(pmidWithCited['pmid'])  for y in pmidWithCited['cited pmcid'][x].split(',') if x not in hisPMID]
    newPmidToCitedPmcid['pmid'] = list(hisPmidToCitedPmcid['pmid']) + [x[0] for x in netData]
    newPmidToCitedPmcid['cited pmcid'] = list(hisPmidToCitedPmcid['cited pmcid']) + [x[1] for x in netData]
    newPmidToCitedPmcid.to_csv('Result\\pmid to cited pmcid.csv',index=None)

    # Pmid to cited pmid
    if os.path.exists('Result\\pmid to cited pmid.csv'):
        hisPmidToCitedPmid = pd.read_csv('Result\\pmid to cited pmid.csv',index_col=None,dtype=str)
        hisPMID = list(set(hisPmidToCitedPmid['pmid']))
    else:
        hisPmidToCitedPmid = pd.DataFrame(columns = ['pmid' , 'cited pmid'])
        hisPMID = []
    newPmidToCitedPmid = pd.DataFrame(columns = ['pmid' , 'cited pmid'])
    pmidWithCited = outputData[outputData['cited pmid']!='None'].copy()
    pmidWithCited.index = list(pmidWithCited['pmid'])
    netData = [[x,y] for x in list(pmidWithCited['pmid'])  for y in pmidWithCited['cited pmid'][x].split(',') if x not in hisPMID]
    newPmidToCitedPmid['pmid'] = list(hisPmidToCitedPmid['pmid']) + [x[0] for x in netData]
    newPmidToCitedPmid['cited pmid'] = list(hisPmidToCitedPmid['cited pmid']) + [x[1] for x in netData]
    newPmidToCitedPmid.to_csv('Result\\pmid to cited pmid.csv',index=None)   




def main(inputFilePath,idListName,apiKey,tool,email,weatherConvertPMCtoPM):
    if inputFilePath.split('.')[-1]=='csv':
        pmidDATA = pd.read_csv(inputFilePath,index_col=None,dtype=str)
        pmidList = [str(x) for x in list(set(pmidDATA[idListName]))]
    elif inputFilePath.split('.')[-1]=='txt':
        pmidDATA = pd.read_table(inputFilePath,index_col=None,dtype=str)
        pmidList = [str(x) for x in list(set(pmidDATA[idListName]))]
    
    # comind with history data
    if os.path.exists('Result\\reference_cited_PMID_Result.txt'):
        hisOutputData = pd.read_table('Result\\reference_cited_PMID_Result.txt',index_col=None,dtype=str)
        hisPMID = list(hisOutputData ['pmid'])
        hisPMIDnoReference = [x for x in hisOutputData ['pmid'][hisOutputData ['reference pmid']=='None']]
        hisPMIDnoCited = [x for x in hisOutputData ['pmid'][hisOutputData ['cited pmcid']=='None']]
    else:
        hisOutputData  = pd.DataFrame(columns=['pmid','reference pmid','cited pmcid','cited pmid'])
        hisPMID , hisPMIDnoReference , hisPMIDnoCited = [],[],[]
    newPMID = [x for x in pmidList if x not in hisPMID]
    newoutputData = pd.DataFrame(columns=['pmid','reference pmid','cited pmcid','cited pmid'])
    newoutputData['pmid'] = newPMID
    outputData = pd.concat( [hisOutputData ,newoutputData], axis=0)
    outputData.index = outputData['pmid']
    print("%d history PMID don't have reference INFO"%(len(hisPMIDnoReference)))
    print("%d history PMID don't have cited     INFO\n"%(len(hisPMIDnoCited)))
    
    # search reference cited 
    referenceSearchResult = getReferencePmid(newPMID+hisPMIDnoReference,apiKey)
    citedSearchResult = getCitedPmid(newPMID+hisPMIDnoCited,apiKey)
    
    # write search result
    for item in referenceSearchResult:
        pmid = item['ids'][0]
        if 'linksetdbs' in item.keys():
            referencePMID = ','.join(item['linksetdbs'][0]['links'])
        else:
            referencePMID = 'None'
        outputData.loc[pmid,'reference pmid'] = referencePMID
    for item in citedSearchResult:
        pmid = item['ids'][0]
        if 'linksetdbs' in item.keys():
            citedPMCID = item['linksetdbs'][0]['links']
            citedPMCID = ','.join(citedPMCID)
        else:
            citedPMCID = 'None'  
        outputData.loc[pmid,'cited pmcid'] = citedPMCID
    outputData.replace(np.nan,'None',inplace=True)
    outputData.to_csv('Result\\reference_cited_PMID_Result.txt',sep='\t',index = False) 
    if weatherConvertPMCtoPM=='True':
        outputData = pd.read_table('Result\\reference_cited_PMID_Result.txt',index_col=None,dtype=str)
        newPmcIDList = []
        for cited_pmcid in outputData['cited pmcid']:
            if cited_pmcid!='None':
                newPmcIDList = list(set(newPmcIDList + cited_pmcid.split(',')))    
        multiTaskConvertPMCtoPM(newPmcIDList,apiKey,tool,email)
        
        pmcidToPmidConverterResultData = pd.read_csv('Result\\pmcid to pmid.csv',index_col=None,dtype=str)
        pmcidToPmidConverterResultData.index = pmcidToPmidConverterResultData['pmcid']
        haveCitedPMCdata = outputData.loc[outputData[outputData['cited pmcid']!='None'].index].copy()
        haveNoCitedPMIDindex = list(haveCitedPMCdata[haveCitedPMCdata['cited pmid']=='None'].index)
        for index in haveNoCitedPMIDindex:
            pmcIDlist = outputData.loc[index]['cited pmcid'].split(',')
            citedPMID = []
            for pmcid in pmcIDlist:
                if pmcid in pmcidToPmidConverterResultData.index:
                    citedPMID.append(pmcidToPmidConverterResultData['pmid'][pmcid])
                else:
                    citedPMID = []
                    break
            if citedPMID != []:
                outputData['cited pmid'][index] = ','.join(citedPMID)             
        outputData.to_csv('Result\\reference_cited_PMID_Result.txt',sep='\t',index = False)
        
    # convert the result to csv type Net file.
    convertToNetFile()
    
if __name__ == '__main__':
    print('InputFile            : %s\ncol_name of pmid     : %s\napikey               : %s\ntool                 : %s\nemail                : %s\nweatherConvertPMCtoPM: %s\n'\
          %(inputFilePath,idListName,apiKey,tool,email,weatherConvertPMCtoPM))
    
    main(inputFilePath,idListName,apiKey,tool,email,weatherConvertPMCtoPM)










