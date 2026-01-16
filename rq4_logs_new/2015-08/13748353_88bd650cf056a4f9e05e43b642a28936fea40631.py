# -*- coding: utf-8 -*-
#!/usr/bin/env python3
#-------------------------------------------------------------------------------
# Name:
# Purpose:       This .py file extracts adjacency lists and detects communities
#                from the corresponding timeslots.
#
# Required libs: python-dateutil,pyparsing,numpy,matplotlib,networkx
# Author:        konkonst
#
# Created:       20/08/2013
# Copyright:     (c) ITI (CERTH) 2013
# Licence:       <apache licence 2.0>
#-------------------------------------------------------------------------------
import json, codecs, os, glob, time, dateutil.parser, collections, datetime, pickle, itertools, math, requests
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import interactive
from operator import itemgetter
import re


class communityranking:
    @classmethod
    def from_json(cls, dataset_path, timeSeg, timeMin, timeMax):

        #Get filenames from json dataset path

        if not os.path.exists(dataset_path + './data/tmp/tweetDict.pck'):
            tweetDict = {'files':[],'tweets':{}}
            tweetDict['tweets'] = {}
            userDict = {}
            alltime, tweetIds = [], []
            totTweets, totMentTws, totNonMentTws, totMents, hashes, urlCount, mediaCount = 0, 0, 0, 0, 0, 0, 0
            stats = [0, 0, 0, 0, 0, 0, 0]
        else:
            tweetDict = pickle.load(open(dataset_path + './data/tmp/tweetDict.pck','rb'))
            userDict = tweetDict['userDict']
            alltime, tweetIds = tweetDict['alltime'], tweetDict['tweetIds']
            stats = tweetDict['stats']
            totTweets, totMentTws, totNonMentTws, totMents, hashes, urlCount, mediaCount = stats[0],stats[1],stats[2],stats[3],stats[4],stats[5],stats[6]

        files = glob.glob(dataset_path + '/data/json/*.json')
        tweetDict['files'] = list(set(tweetDict['files']))
        files = [x for x in files if x not in tweetDict['files']]

        '''Parse the json files into authors/mentions/alltime/hashtags/tweetIds/text lists'''
        for filename in files:
            flag = False
            # with codecs.open(filename, 'r', 'utf8') as f:
            with open(filename, 'r') as f:
                for line in f:
                    # read_line = line.strip().encode('utf-8')
                    # json_line = json.loads(read_line.decode('utf-8'))
                    json_line = json.loads(line.strip())
                    try:
                        dt = dateutil.parser.parse(json_line['created_at'],dayfirst=True)
                        mytime = int(time.mktime(dt.timetuple()))
                        if mytime >= timeMin and mytime <= timeMax:
                            try:
                                json_line['entities']['user_mentions'][0]
                                len_ment = len(json_line['entities']['user_mentions'])
                                tmpMents = []
                                for i in range(len_ment):  
                                    tmpMents.append(json_line['entities']['user_mentions'][i]['screen_name'])                       
                                    if json_line['entities']['user_mentions'][i]['screen_name'] not in userDict:
                                        userDict[json_line['entities']['user_mentions'][i]['screen_name']] = {'id':json_line['entities']['user_mentions'][i]['id'],'followers_count':0,
                                        'listed_count':0,'friends_count':0,'description':'','name':json_line['entities']['user_mentions'][i]['name'],'location':'','statuses_count':0}
                                        totMents += 1     
                                tweetDict['tweets'][json_line['id_str']] = {}    
                                tweetDict['tweets'][json_line['id_str']]['user_mentions'] = tmpMents
                                totMentTws += 1 
                                alltime.append(mytime)
                                tweetDict['tweets'][json_line['id_str']]['time'] = mytime
                                tweetIds.append(json_line['id_str'])   
                                tweetDict['tweets'][json_line['id_str']]['authors'] = json_line['user']['screen_name']
                                userDict[json_line['user']['screen_name']] = {'id':json_line['user']['id'],'followers_count':json_line['user']['followers_count'],
                                    'listed_count':json_line['user']['listed_count'],'friends_count':json_line['user']['friends_count'],'description':json_line['user']['description'],
                                    'name':json_line['user']['name'],'location':json_line['user']['location'],'statuses_count':json_line['user']['statuses_count']}
                                tweetDict['tweets'][json_line['id_str']]['text'] = json_line['text']
                                try:
                                    tmp = []
                                    for textIdx in json_line['entities']['hashtags']:
                                        hashes += 1
                                        tmp.append(textIdx['text'])
                                    tweetDict['tweets'][json_line['id_str']]['hashtags'] = tmp
                                except:
                                    pass
                                try:
                                    tmp = []
                                    for textIdx in json_line['entities']['urls']:
                                        urlCount += 1
                                        tmp.append(textIdx['expanded_url'])
                                    tweetDict['tweets'][json_line['id_str']]['urls'] = tmp
                                except:
                                    pass
                                try:
                                    tmp = []
                                    for textIdx in json_line['extended_entities']['media']:
                                        mediaCount +=1
                                        tmp.append(textIdx['type'])
                                except:
                                    tweetDict['tweets'][json_line['id_str']]['media'] = []
                                    pass
                            except:
                                totNonMentTws += 1
                                pass
                            totTweets += 1
                        else:
                            if mytime > timeMax:
                                flag = True
                                continue
                    except:
                        print('bad tweet')
                        pass
            f.close()

        tweetDict['userDict'] = userDict
        zippedall=zip(alltime,tweetIds)
        zippedall=sorted(zippedall)
        alltime, tweetIds = zip(*zippedall)
        alltime, tweetIds = list(alltime), list(tweetIds)
        tweetDict['alltime'], tweetDict['tweetIds'] = alltime, tweetIds
        stats[0],stats[1],stats[2],stats[3],stats[4],stats[5],stats[6] = totTweets, totMentTws, totNonMentTws, totMents, hashes, urlCount, mediaCount
        tweetDict['stats'] = stats

        pickle.dump(tweetDict, open(dataset_path + './data/tmp/tweetDict.pck','wb'), protocol = 2)
        
        statsfile = open(dataset_path + '/data/results/basicstats.txt','w')
        statement = ('Total # of Tweets= ' + str(totTweets) + '\nTotal # of Tweets with mentions: ' +
            str(totMentTws) + '\nTotal # of Tweets without mentions: ' + str(totNonMentTws) +
            '\nTotal # of edges: ' + str(totMents) +
            '\nTotal # of hashtags: ' + str(hashes) +
            '\nTotal # of urls: ' + str(urlCount) + 
            '\nTotal # of media: ' + str(mediaCount) + '\n')
        print(statement)
        statsfile.write(statement)
        statsfile.close()

        return cls(alltime, tweetIds, dataset_path, timeSeg),tweetDict

    def __init__(self, alltime, tweetIds, dataset_path, timeSeg):
        self.alltime = alltime
        self.tweetIds = tweetIds
        self.dataset_path = dataset_path
        self.timeSeg = timeSeg
        self.usersPerTmsl = {}
        self.userPgRnkBag = {}
        self.commBag = {}
        self.urlBag = {}
        self.adjListBag = {}
        self.commBetweenessBag = {}

    def timeslotselection(self):
        ###Parsing commences###

        # Create time segments a human can understand
        humanTimeSegs=[]
        for idx,seg in enumerate(self.timeSeg):
            if seg <3600:
                timeNum = seg / 60
                timeTitle = ' mins'
                humanTimeSegs.append(str(idx+1)+'> '+str(round(timeNum))+timeTitle)
            elif seg >= 3600 and seg < 86400:
                timeNum = seg / 3600
                timeTitle = ' hours'
                humanTimeSegs.append( str(idx+1)+'> '+str(round(timeNum))+timeTitle)
            elif seg >= 86400 and seg < 604800:
                timeNum = seg / 86400
                timeTitle = ' days'
                humanTimeSegs.append(str(idx+1)+'> '+str(round(timeNum))+timeTitle)
            elif seg / 86400 == 1:
                timeTitle = ' day'
                humanTimeSegs.append(str(idx+1)+'> '+str(round(timeNum))+timeTitle)
            elif seg >= 604800 and seg < 2592000:
                timeNum = seg / 604800
                timeTitle = ' weeks'
                humanTimeSegs.append(str(idx+1)+'> '+str(round(timeNum))+timeTitle)
            else:
                timeNum = seg / 2592000
                timeTitle = ' months'
                humanTimeSegs.append(str(idx+1)+'> '+str(round(timeNum))+timeTitle)


        #Find time distance between posts#
        time2 = np.append(self.alltime[0], self.alltime)
        time2 = time2[0:len(time2) - 1]
        timeDif = self.alltime - time2
        lT = len(self.alltime)

        '''Extract the first derivative'''
        font = {'size': 14}
        plt.rc('font', **font)
        fig = plt.figure()#figsize=(10,8)
        plotcount, globfirstderiv, globmentionLimit = 0, {}, {}
        for seg in self.timeSeg:
            if seg <3600:
                timeNum = seg / 60
                timeTitle = ' mins'
                labelstr = '%H:%M'
            elif seg >= 3600 and seg < 86400:
                timeNum = seg / 3600
                timeTitle = ' hours'
                labelstr = '%Hh/%d'
            elif seg >= 86400 and seg < 604800:
                timeNum = seg / 86400
                timeTitle = ' days'
                labelstr = '%d/%b'
            elif seg / 86400 == 1:
                timeTitle = ' day'
                labelstr = '%d/%b'
            elif seg >= 604800 and seg < 2592000:
                timeNum = seg / 604800
                timeTitle = ' weeks'
                labelstr = '%d/%b'
            else:
                timeNum = seg / 2592000
                timeTitle = ' months'
                labelstr = '%b/%y'

            curTime, bin, freqStat, mentionLimit, timeLabels = 0, 0, [0], [], []
            for i in range(lT):
                curTime += timeDif[i]
                if curTime <= seg:
                    freqStat[bin] += 1
                else:
                    curTime = 0
                    mentionLimit = np.append(mentionLimit, i)
                    timeLabels = np.append(timeLabels, datetime.datetime.fromtimestamp(self.alltime[i]).strftime(labelstr))
                    bin += 1
                    freqStat = np.append(freqStat, 0)
            mentionLimit = np.append(mentionLimit, i)            
            timeLabels = np.append(timeLabels, datetime.datetime.fromtimestamp(self.alltime[-1]).strftime(labelstr))
            freqStatIni = np.zeros(len(freqStat) + 1)
            freqStatMoved = np.zeros(len(freqStat) + 1)
            freqStatIni[0:len(freqStat)] = freqStat
            freqStatMoved[1:len(freqStat) + 1] = freqStat
            firstderiv = freqStatIni - freqStatMoved
            firstderiv[len(firstderiv) - 1] = 0

            globfirstderiv[seg] = firstderiv
            globmentionLimit[seg] = mentionLimit

            plotcount += 1

            if len(self.timeSeg) < 3:
                ax = fig.add_subplot(2, int(np.ceil(len(self.timeSeg) / 2)), plotcount, autoscale_on=True)
            else:
                ax = fig.add_subplot(int(np.ceil(len(self.timeSeg) / 2)), 2, plotcount, autoscale_on=True)
            plt.grid(axis='x')
            plt.plot(freqStat, 'b-', hold=True)
            plt.ylabel('User activity (mentions)')
            plt.xlabel('Init. time: ' + datetime.datetime.fromtimestamp(int(self.alltime[0])).strftime('%H:%M-%d/%m/%y')+ ', Last point:'+ datetime.datetime.fromtimestamp(int(self.alltime[-1])).strftime('%H:%M-%d/%m/%y') + ' (Ts:' + str(round(timeNum)) + timeTitle + ')')
            poi = []
            for k in range(len(mentionLimit)):
                if firstderiv[k] < 0 <= firstderiv[k + 1]:
                    poi = np.append(poi, k)
            poi = np.int32(poi)
            plt.plot(poi, freqStat[poi], 'ro', hold=True)
            pertick=np.ceil(len(freqStat)/self.xLablNum)
            ax.set_xticks(np.arange(0, len(freqStat), pertick))#, minor=False)
            ax.set_xticklabels(timeLabels[0::pertick], minor=False, fontsize = 14, rotation = 35)
            plt.xlim(xmax=(len(freqStat)))
        mng = plt.get_current_fig_manager()
        mng.resize(*mng.window.maxsize())
        interactive(True)
        plt.show()
        plt.savefig(self.dataset_path + '/data/results/tweet_activity.pdf', bbox_inches='tight', format='pdf')
        if len(self.timeSeg) > 1:
            timeSegInput = int(input('Please select sampling time: \n' + str(humanTimeSegs)))
        else:
            timeSegInput = 1
        timeSegInput=self.timeSeg[timeSegInput-1]
        plt.close()
        del(fig,self.timeSeg)
        if timeSegInput < 3600:
            timeNum = timeSegInput / 60
            timeTitle = 'per' + str(int(timeNum)) + 'mins'
            labelstr = '%H:%M'
        elif timeSegInput >= 3600 and timeSegInput < 86400:
            timeNum = timeSegInput / 3600
            timeTitle = 'per' + str(int(timeNum)) + 'hours'
            labelstr = '%Hh/%d'
        elif timeSegInput >= 86400 and timeSegInput < 604800:
            timeNum = timeSegInput / 86400
            timeTitle = 'per' + str(int(timeNum)) + 'days'
            labelstr = '%d/%b'
        elif timeSegInput>= 604800 and timeSegInput < 2592000:
            timeNum = timeSegInput / 604800
            timeTitle = 'per' + str(int(timeNum)) + 'weeks'
            labelstr = '%d/%b'
        else:
            timeNum = timeSegInput / 2592000
            timeTitle = 'per' + str(int(timeNum)) + 'months'
            labelstr = '%b/%y'

        self.fileTitle = timeTitle
        self.labelstr = labelstr
        firstderiv = globfirstderiv[timeSegInput]
        mentionLimit = globmentionLimit[timeSegInput]
        return firstderiv, mentionLimit

    def extraction(self):
        '''Extract adjacency lists,mats,user and community centrality and communities bags'''
        import igraph

        #Compute the first derivative and the point of timeslot separation
        firstderiv, mentionLimit = self.timeslotselection()
        t = time.time()

        #Extract unique users globally and construct dictionary
        authors, mentions = [], []
        for x in self.tweetDict['tweets'].keys():
            authors.append(self.tweetDict['tweets'][x]['authors'])
            mentions.append(self.tweetDict['tweets'][x]['user_mentions'])
        mentions = list(itertools.chain.from_iterable(mentions))
        usrs = authors.copy()
        usrs.extend(mentions)
        usrs = list(set(usrs))
        usrs.sort()
        self.uniqueUsers = {x:num for num,x in enumerate(usrs)}

        statement = 'Total # of unique users: %s\n' %len(self.uniqueUsers)
        statsfile = open(self.dataset_path + '/data/results/basicstats.txt','a')
        print(statement)
        statsfile.write(statement)
        statsfile.close()

        #Split time according to the first derivative of the users' activity#
        sesStart, timeslot, timeLimit,commCount = 0, 0, [], 0        
        self.commPgRnkBag, self.commPgRnkBagNormed, self.authorTwIdPerTmslDict = {}, {}, {}
        print('Forming timeslots')
        for k in range(len(mentionLimit)):
            if self.adaptive:
                locMin = firstderiv[k] < 0 and firstderiv[k + 1] >= 0
                if  locMin or k == len(mentionLimit)-1:
                    del(locMin)
                else:
                    continue
            #make timeslot timelimit array
            timeLimit.append(self.alltime[int(mentionLimit[k])])
            fileNum = '{0}'.format(str(timeslot).zfill(2))
            sesEnd = int(mentionLimit[k] + 1)

            tweetTempList = self.tweetIds[sesStart:sesEnd]
            #Make pairs of users with weights
            authors, mentions = [], []
            self.authorTwIdPerTmslDict[timeslot] = {}
            for twId in tweetTempList:
                if self.tweetDict['tweets'][twId]['authors'] not in self.authorTwIdPerTmslDict[timeslot]:
                    self.authorTwIdPerTmslDict[timeslot][self.tweetDict['tweets'][twId]['authors']] = [twId]
                else:
                    self.authorTwIdPerTmslDict[timeslot][self.tweetDict['tweets'][twId]['authors']].append(twId)
                for m in self.tweetDict['tweets'][twId]['user_mentions']:
                    authors.append(self.tweetDict['tweets'][twId]['authors'])
                    mentions.append(m)
            usersPair = list(zip(authors,mentions))
            #Create weighted adjacency list
            weighted = collections.Counter(usersPair)
            weighted = list(weighted.items())
            adjusrs, weights = zip(*weighted)
            adjauthors, adjments = zip(*adjusrs)
            adjList = list(zip(adjauthors, adjments, weights))

            print('For Timeslot: '+str(fileNum)+' comprising '+str(len(adjList))+' edges.')

            self.usersPerTmsl[timeslot] = list(set(itertools.chain.from_iterable([authors,mentions])))

            '''Write pairs of users to txt file for Gephi'''            
            if not os.path.exists(self.dataset_path + '/data/results/'+self.adaptStr+'/forGephi/'+str(self.fileTitle)):
                os.makedirs(self.dataset_path + '/data/results/'+self.adaptStr+'/forGephi/'+str(self.fileTitle))
            my_txt = open(self.dataset_path + '/data/results/'+self.adaptStr+'/forGephi/'+str(self.fileTitle)+'/usersPairs_' + fileNum +'.txt', 'w')#
            my_txt.write('Source,Target,Weight' + '\n')
            for line in adjList:
                my_txt.write(','.join(str(x) for x in line) + '\n')
            my_txt.close()

            self.adjListBag[timeslot] = adjList

            #Construct igraph graph  
            # edgelist = [(uniqueIds[u], uniqueIds[v]) for u, v, _ in adjList]
            # weights = [w for _, _, w in adjList]
            gDirected=igraph.Graph.TupleList(adjList, directed = True, weights=True)     
            gDirected.simplify(multiple=False, loops=True, combine_edges=False)
            # gUndirected=igraph.Graph.TupleList(adjList, weights=True)

            #Extract the centrality of each user using the PageRank algorithm      
            igraphUserPgRnk = gDirected.pagerank(weights = 'weight')    

            pgRnkMax = max(igraphUserPgRnk) 

            usrlist = gDirected.vs['name']
            tempUserPgRnk = {}
            for i,k in enumerate(usrlist):
                tempUserPgRnk[k] = igraphUserPgRnk[i]#/pgRnkMax
            self.userPgRnkBag[timeslot] = tempUserPgRnk

            #Detect Communities using the louvain algorithm# 
            # louvComms = gUndirected.community_multilevel(weights = 'weight')
            extractedComms = gDirected.community_infomap(edge_weights = 'weight')
            strCommDict, numCommDict, twIdCommDict = {}, {}, {}
            for k, v in enumerate(extractedComms.membership):
                strCommDict[v] = strCommDict.get(v, [])
                strCommDict[v].append(usrlist[k])
                strCommDict[v].sort()
                numCommDict[v] = numCommDict.get(v, [])
                numCommDict[v].append(self.uniqueUsers[usrlist[k]])
                numCommDict[v].sort()
                try:
                    self.authorTwIdPerTmslDict[timeslot][usrlist[k]]
                    twIdCommDict[v] = twIdCommDict.get(v, [])
                    twIdCommDict[v].extend(self.authorTwIdPerTmslDict[timeslot][usrlist[k]])
                    twIdCommDict[v].sort()
                except:
                    pass
            commCount+=len(strCommDict)

            self.commBag[timeslot] = {}
            self.commBag[timeslot]['strComms'] = strCommDict
            self.commBag[timeslot]['numComms'] = numCommDict
            self.commBag[timeslot]['tweetIds'] = twIdCommDict

            #Construct a graph using the communities as users
            tempCommGraph = extractedComms.cluster_graph(combine_edges = False) 
            tempAllCommGraphs = extractedComms.subgraphs()
            tmprecipr = []
            for x in tempAllCommGraphs:
                if math.isnan(x.reciprocity()):
                    tmprecipr.append(0)
                else:
                    tmprecipr.append(x.reciprocity())
            tempCommGraph.simplify(multiple=False, loops=True, combine_edges=False)
            self.commBag[timeslot]['commEdgesOut'],self.commBag[timeslot]['commEdgesIn'] = {},{}
            for idx, commAdj in enumerate(tempCommGraph.get_adjlist(mode='ALL')):
                self.commBag[timeslot]['commEdgesOut'][idx] = []
                self.commBag[timeslot]['commEdgesIn'][idx] = []
                for x in commAdj:
                    if x!=idx:
                        self.commBag[timeslot]['commEdgesOut'][idx].append(x)
                    else:
                        self.commBag[timeslot]['commEdgesIn'][idx].append(x)

            # self.commBag[timeslot]['Similarity_Jaccard'] = tempCommGraph.similarity_jaccard()
            self.commBag[timeslot]['indegree'] = tempCommGraph.indegree()
            self.commBag[timeslot]['outdegree'] = tempCommGraph.outdegree()
            self.commBag[timeslot]['reciprocity'] = tmprecipr

            #Detect the centrality of each community using the PageRank algorithm
            commPgRnk = tempCommGraph.pagerank(weights = 'weight')
            minCPGR = min(commPgRnk)
            self.commPgRnkBag[timeslot] = commPgRnk
            self.commPgRnkBagNormed[timeslot] = [v/minCPGR for v in commPgRnk]

            #Detect the centrality of each community using the betweeness centrality algorithm
            commBetweeness = tempCommGraph.betweenness(weights = 'weight', directed = True)
            self.commBetweenessBag[timeslot] = commBetweeness

            #Extract community degree
            degreelist= tempCommGraph.degree(loops = False)
            self.commBag[timeslot]['alldegree'] = degreelist

            sesStart = sesEnd
            timeslot += 1

        self.timeslots=timeslot

        day_month = [datetime.datetime.fromtimestamp(int(self.alltime[0])).strftime(self.labelstr)]
        day_month.extend([datetime.datetime.fromtimestamp(int(x)).strftime(self.labelstr) for x in timeLimit])
        self.day_month = day_month
        self.timeLimit = timeLimit
        statement = '\nTotal # of communities is '+str(commCount) + '\n'
        statsfile = open(self.dataset_path + '/data/results/'+self.adaptStr+'/commstats_'+self.fileTitle+'.txt','w')
        print(statement)
        statsfile.write(statement)
        statsfile.close()

        dataCommPck = open(self.dataset_path + '/data/tmp/'+self.adaptStr+'/dataComm_'+str(self.fileTitle)+'.pck','wb')
        pickle.dump(self, dataCommPck , protocol = 2)
        dataCommPck.close()

        elapsed = time.time() - t
        print('Stage 2 took: %.2f seconds' % elapsed)

    def evol_detect(self, prevTimeslots, xLablNum, adaptive):        
        import random
        self.xLablNum=xLablNum

        self.adaptive = adaptive
        if adaptive:
            self.adaptStr = 'adaptive'
        else:
            self.adaptStr = ''
        
        try:
            self.commPgRnkBag            
            print('Comms have already been extracted. Moving to Stage 3...')
        except:            
            print('Comms have not been extracted. Moving to Stage 2...')
            self.extraction()
            pass

        timeslots = self.timeslots

        '''find out the users that appear in more that one timestamps'''
        countedTmslUsers = collections.Counter(list(itertools.chain.from_iterable(self.usersPerTmsl.values())))

        '''Construct Community Dictionary'''
        # print('Constructing Community Dictionary')
        commSizeBag = {}
        lC = [] #Number of communities>2people for each timeslot
        for cBlen in range(timeslots):
            commStrBag2 = dict(self.commBag[cBlen]['strComms'])
            commSizeBag[cBlen] = {}            
            for k,v in commStrBag2.items():
                croppedv = [x for x in v if countedTmslUsers[x] > 1]
                lenV = len(croppedv)
                if lenV < 3:
                    del(self.commBag[cBlen]['strComms'][k])
                    del(self.commBag[cBlen]['numComms'][k])
                    del(self.commBag[cBlen]['commEdgesOut'][k])#cut out communities that contain users that do not appear in more than one timeslots
                    del(self.commBag[cBlen]['commEdgesIn'][k])
                    try:
                        del(self.commBag[cBlen]['tweetIds'][k])
                    except:
                        pass
                else:
                    commSizeBag[cBlen][k] = len(v)
                    self.commPgRnkBag[cBlen]

            lC.append(len(self.commBag[cBlen]['strComms']))

        # '''Fix Borda count '''
        # bordaCentralityBag = {}
        # for cBlen in range(timeslots):

        statement = '\nTotal # of reduced communities is '+str(sum(lC)) + '\n'
        statsfile = open(self.dataset_path + '/data/results/'+self.adaptStr+'/commstats_'+self.fileTitle+'.txt','a')
        print(statement)
        statsfile.write(statement)
        statsfile.close()
        self.commPerTmslt=lC

        #Detect any evolution and name the evolving communities
        #uniCommIdsEvol is structured as such {'Id':[rowAppearence],[commCentrality],[commSize],[users]}        
        self.commTweetBag, self.commHashtagBag, self.commTweetIdBag, self.commUrlBag = {}, {}, {}, {}
        evolcounter, uniCommIdsEvol, commCntr, dynCommCount, commIds = 0, {}, 0, 0, []
        thres = 0.2
        print('Community similarity search...')
        t = time.time()
        for rows in range(1, timeslots):
            print('...for timeslot: '+str(rows)+' of '+str(timeslots-1))            
            t2 = time.time()
            for clmns,bag1 in self.commBag[rows]['numComms'].items():
                # idx = str(rows) + ',' + str(clmns)
                tempcommSize = len(bag1)
                for invrow in range(1, prevTimeslots + 1):
                    prevrow = rows - invrow                   
                    tmpsim = {}
                    if prevrow >= 0:           
                        for clmns2,prevComms in self.commBag[prevrow]['numComms'].items():
                            lenprevComms = len(prevComms)
                            # tmpratio = lenprevComms / tempcommSize
                            tmpratio = min(tempcommSize,lenprevComms)/max(tempcommSize,lenprevComms)
                            if thres >= tmpratio or thres >= 1/tmpratio:
                                continue
                            else:
                                sim = len(set(bag1).intersection(prevComms)) / len(set(np.append(bag1, prevComms)))
                                if sim > thres:
                                    tmpsim[clmns2] = sim
                        if tmpsim:
                            tmpsim = {x:v+round(random.random()/10000,5) for x,v in tmpsim.items()}
                            maxval = max(tmpsim.values())
                        else:
                            maxval = 0
                        if maxval >= thres:
                            dynCommCountList = []
                            for idx, val in tmpsim.items():
                                if str(prevrow) + ',' + str(idx) not in commIds:                                    
                                    evolcounter += 1
                                    uniCommIdsEvol[dynCommCount] = [[], [], [], [], [], [], [], [], [], [], [], [], [], []]
                                    uniCommIdsEvol[dynCommCount][0].append(prevrow)#timeslot num for first evolution
                                    uniCommIdsEvol[dynCommCount][1].append(self.commPgRnkBag[prevrow][idx])#community pagerank for first evolution
                                    uniCommIdsEvol[dynCommCount][2].append(commSizeBag[prevrow][idx])#community size per timeslot for first evolution
                                    uniCommIdsEvol[dynCommCount][3].append(self.commBag[prevrow]['strComms'][idx])#users in each community for first evolution
                                    uniCommIdsEvol[dynCommCount][4].append(self.commBag[prevrow]['alldegree'][idx])#community degree for first evolution
                                    uniCommIdsEvol[dynCommCount][5].append(self.commPgRnkBagNormed[prevrow][idx])#normed community pagerank for first evolution
                                    uniCommIdsEvol[dynCommCount][6].append(self.commBetweenessBag[prevrow][idx])#community betweeness centrality for first evolution
    								#uniCommIdsEvol[dynCommCount][7].append(0)
                                    uniCommIdsEvol[dynCommCount][8].append(str(prevrow) + ',' + str(idx))#community names in between
                                    uniCommIdsEvol[dynCommCount][9].append(self.commBag[prevrow]['commEdgesOut'][idx])
                                    uniCommIdsEvol[dynCommCount][10].append(self.commBag[prevrow]['indegree'][idx])#indegree of community
                                    uniCommIdsEvol[dynCommCount][11].append(self.commBag[prevrow]['outdegree'][idx])#outdegree of community
                                    uniCommIdsEvol[dynCommCount][12].append(self.commBag[prevrow]['reciprocity'][idx])#reciprocity of community                                    
                                    uniCommIdsEvol[dynCommCount][13].append(self.commBag[prevrow]['commEdgesIn'][idx])
                                    commIds.append(str(prevrow) + ',' + str(idx))
                                    dynCommCountList.append(dynCommCount)
                                    tmpTw, tmpHa, tmptwId, tmpUrl = [], [], [], []
                                    self.commTweetBag[dynCommCount], self.commHashtagBag[dynCommCount], self.commTweetIdBag[dynCommCount], self.commUrlBag[dynCommCount] = [], [], [], []
                                    try:
                                        for twId in self.commBag[prevrow]['tweetIds'][idx]:
                                            tmptwId.append(twId)
                                            tmpTw.append(self.tweetDict['tweets'][twId]['text'])
                                            tmpHa.extend(self.tweetDict['tweets'][twId]['hashtags'])
                                            tmpUrl.extend(self.tweetDict['tweets'][twId]['urls'])
                                    except:
                                        pass
                                    self.commTweetBag[dynCommCount].append(tmpTw)
                                    self.commHashtagBag[dynCommCount].append(tmpHa)
                                    self.commTweetIdBag[dynCommCount].append(tmptwId)
                                    self.commUrlBag[dynCommCount].append(tmpUrl)                                    
                                    dynCommCount += 1
                                    commCntr += 1                          
                                else:
                                    for dyn, innerDict in uniCommIdsEvol.items():
                                        if str(prevrow) + ',' + str(idx) in innerDict[8]:
                                            dynCommCountList.append(dyn)
                            for d in dynCommCountList:
                                uniCommIdsEvol[d][0].append(rows)#timeslot num
                                uniCommIdsEvol[d][1].append(self.commPgRnkBag[rows][clmns])#community pagerank per timeslot
                                uniCommIdsEvol[d][2].append(commSizeBag[rows][clmns])#community size per timeslot
                                uniCommIdsEvol[d][3].append(self.commBag[rows]['strComms'][clmns])#users in each community
                                uniCommIdsEvol[d][4].append(self.commBag[rows]['alldegree'][clmns])#community degree per timeslot
                                uniCommIdsEvol[d][5].append(self.commPgRnkBagNormed[rows][clmns])#normed community pagerank per timeslot
                                uniCommIdsEvol[d][6].append(self.commBetweenessBag[rows][clmns])#community betweeness centrality per timeslot
                                uniCommIdsEvol[d][7].append(val)#similarity between the two communities in evolving timesteps
                                uniCommIdsEvol[d][8].append(str(rows) + ',' + str(clmns))#community names in between
                                uniCommIdsEvol[d][9].append(self.commBag[rows]['commEdgesOut'][clmns])
                                uniCommIdsEvol[d][10].append(self.commBag[rows]['indegree'][clmns])#indegree of community
                                uniCommIdsEvol[d][11].append(self.commBag[rows]['outdegree'][clmns])#outdegree of community
                                uniCommIdsEvol[d][12].append(self.commBag[rows]['reciprocity'][clmns])#reciprocity of community
                                uniCommIdsEvol[d][13].append(self.commBag[rows]['commEdgesIn'][clmns])
                                commIds.append(str(rows) + ',' + str(clmns))
                                tmpTw, tmpHa, tmptwId, tmpUrl = [], [], [], []
                                try:
                                    for twId in self.commBag[rows]['tweetIds'][clmns]:
                                        tmptwId.append(twId)
                                        tmpTw.append(self.tweetDict['tweets'][twId]['text'])
                                        tmpHa.extend(self.tweetDict['tweets'][twId]['hashtags'])
                                        tmpUrl.extend(self.tweetDict['tweets'][twId]['urls'])
                                except:
                                    pass
                                self.commTweetBag[d].append(tmpTw)
                                self.commHashtagBag[d].append(tmpHa)
                                self.commTweetIdBag[d].append(tmptwId)
                                self.commUrlBag[d].append(tmpUrl)    
                                commCntr += 1      
                            break
            elapsed = time.time() - t2
            print('Elapsed: %.2f seconds' % elapsed)
        uniCommIds = list(uniCommIdsEvol.keys())
        uniCommIds.sort()

        elapsed = time.time() - t
        print('Elapsed: %.2f seconds' % elapsed)

        self.uniCommIds,self.uniCommIdsEvol=uniCommIds,uniCommIdsEvol

        del(commIds,self.alltime,self.commBetweenessBag,commSizeBag)#,self.commPgRnkBag,self.commBag,)

        statement = (str(evolcounter) + ' evolutions and ' + str(len(uniCommIds)) + ' dynamic communities and ' + str(commCntr)+' evolving communities' + '\n')
        statsfile = open(self.dataset_path + '/data/results/'+self.adaptStr+'/commstats_'+self.fileTitle+'.txt','a')
        print(statement)
        statsfile.write(statement)
        statsfile.close()
        return self

    def commRanking(self,numTopComms, prevTimeslots,xLablNum):
        import tfidf, random, twython, nltk, urllib.parse
        from pymongo import MongoClient
        from nltk.corpus import stopwords

        regex1 = re.compile("(?:\@|#|https?\://)\S+",re.UNICODE)
        regex2 = re.compile("\w+'?\w+",re.UNICODE)
        
        '''Detect the evolving communities'''
        uniCommIdsEvol=self.uniCommIdsEvol
        timeslots=self.timeslots

        tempcommRanking = {}
        #structure: tempcommRanking={Id:[persistence,stability,commCentrality,degreeness]}
        definiteStop = ['gt','amp','rt','via']
        commRanking, rankingDict, lifetime, simpleEntropyDict, bigramEntropyDict = {}, {},0, {}, {}
        for Id in self.uniCommIds:
            
            tempcommRanking[Id] = []
            rankingDict[Id] = {}  
            uniqueTimeSlLen = len(set(uniCommIdsEvol[Id][0]))
            timeSlLen=len(uniCommIdsEvol[Id][0])

            # '''Checking Theseus Ship'''
            rankingDict[Id]['theseus'] = 1+len(set(uniCommIdsEvol[Id][3][0]).intersection(uniCommIdsEvol[Id][3][-1])) #/ len(set(np.append(uniCommIdsEvol[Id][3][0], uniCommIdsEvol[Id][3][-1])))
            '''text entropy extraction'''
            tmptextlist = [[i for i in regex2.findall(regex1.sub('',' '.join(x).lower())) if i and not i.startswith(('rt','htt','(@','\'@','t.co')) and len(i)>2 and i not in definiteStop] for x in self.commTweetBag[Id]]
            simpleEntropyDict[Id] = [myentropy(x) for x in tmptextlist]
            bigramList = [[' '.join(x) for x in list(nltk.bigrams(tmpTopic))] for tmpTopic in tmptextlist]
            bigramEntropyDict[Id] = [myentropy(x) for x in bigramList]
            rankingDict[Id]['avgBigramTextentropy'] = sum(bigramEntropyDict[Id])/timeSlLen
            rankingDict[Id]['textentropy'] = sum(simpleEntropyDict[Id])/timeSlLen
            rankingDict[Id]['size'] = sum(uniCommIdsEvol[Id][2]) / uniqueTimeSlLen
            rankingDict[Id]['persistence'] = uniqueTimeSlLen / timeslots #persistence)
            rankingDict[Id]['stability'] = (sum(np.diff(list(set(uniCommIdsEvol[Id][0]))) == 1) + 1) / (timeslots + 1) #stability
            rankingDict[Id]['perstability'] = rankingDict[Id]['stability']*rankingDict[Id]['persistence']  #perstability)
            rankingDict[Id]['commCentrality'] = sum(uniCommIdsEvol[Id][1]) / uniqueTimeSlLen #commCentrality
            rankingDict[Id]['commCentralityNormed'] = sum(uniCommIdsEvol[Id][5]) / uniqueTimeSlLen #normed commCentrality
            rankingDict[Id]['commMaxCentralityNormed'] = max(uniCommIdsEvol[Id][5]) #max normed commCentrality
            rankingDict[Id]['betweeness'] = sum(uniCommIdsEvol[Id][6])#/ uniqueTimeSlLen #betweeness
            rankingDict[Id]['connections'] = sum([len(y) for y in [set(x) for x in uniCommIdsEvol[Id][9]]])/ uniqueTimeSlLen #connections to other communities
            rankingDict[Id]['urlAvg'] = sum([len(set(y)) for y in self.commUrlBag[Id]]) / uniqueTimeSlLen #average number of unique urls in every community
            rankingDict[Id]['similarityAvg'] = sum(uniCommIdsEvol[Id][7]) / uniqueTimeSlLen #average jaccardian between timeslots for each dyn comm
            rankingDict[Id]['reciprocity'] = sum(uniCommIdsEvol[Id][12]) / uniqueTimeSlLen #average reciprocity between timeslots for each dyn comm

        '''Comms ranked in order of features'''
        rankedPersistence = sorted(rankingDict, key=lambda k: [rankingDict[k]['persistence'],rankingDict[k]['stability'],rankingDict[k]['connections'],rankingDict[k]['commCentralityNormed']], reverse = True)
        rankedStability = sorted(rankingDict, key=lambda k: [rankingDict[k]['stability'],rankingDict[k]['persistence'],rankingDict[k]['connections'],rankingDict[k]['commCentralityNormed']], reverse = True)
        rankedPerstability = sorted(rankingDict, key=lambda k: [rankingDict[Id]['perstability'],rankingDict[k]['connections'],rankingDict[k]['commCentralityNormed']], reverse = True)
        rankedcommCentrality = sorted(rankingDict, key=lambda k: [rankingDict[k]['commCentrality'],rankingDict[k]['connections'],rankingDict[k]['size']], reverse = True)
        rankedcommBetweeness = sorted(rankingDict, key=lambda k: [rankingDict[k]['betweeness'],rankingDict[k]['size'],rankingDict[k]['connections']], reverse = True)
        rankedcommCentralityNormed = sorted(rankingDict, key=lambda k: [rankingDict[k]['commCentralityNormed'],rankingDict[k]['connections'],rankingDict[k]['size']], reverse = True)
        rankedcommMaxCentralityNormed = sorted(rankingDict, key=lambda k: [rankingDict[k]['commMaxCentralityNormed'],rankingDict[k]['connections'],rankingDict[k]['size']], reverse = True)
        rankedTheseus = sorted(rankingDict, key=lambda k: [rankingDict[k]['theseus'],rankingDict[k]['connections'],rankingDict[k]['commCentralityNormed']], reverse = True)
        rankedConnections = sorted(rankingDict, key=lambda k: [rankingDict[k]['connections'],rankingDict[k]['size'],rankingDict[k]['commCentralityNormed']], reverse = True)
        rankedcommSize = sorted(rankingDict, key=lambda k: [rankingDict[k]['size'],rankingDict[k]['connections'],rankingDict[k]['commCentralityNormed']], reverse = True)     
        rankedtextentropy = sorted(rankingDict, key=lambda k: [rankingDict[Id]['textentropy'],rankingDict[k]['size'],rankingDict[k]['commMaxCentralityNormed']], reverse = True)   
        rankedUrlAvg = sorted(rankingDict, key=lambda k: [rankingDict[k]['urlAvg'],rankingDict[k]['size'],rankingDict[k]['commMaxCentralityNormed']], reverse = True)
        rankedSimilarityAvg = sorted(rankingDict, key=lambda k: [rankingDict[k]['similarityAvg'],rankingDict[k]['commMaxCentralityNormed']], reverse = True)
        rankedReciprocity = sorted(rankingDict, key=lambda k: [rankingDict[k]['reciprocity'],rankingDict[k]['connections']], reverse = True)
        commRanking = {}
        whichmethod = 'TISCI'# size centrality perstability TISCI
        for Id in self.uniCommIds:#rankedPersistence.index(Id),rankedStability.index(Id),rankedcommBetweeness.index(Id),rankedcommMaxCentralityNormed.index(Id),rankedUrlAvg.index(Id),rankedConnections.index(Id)
            if whichmethod == 'TISCI':
                commRanking[Id] = recRank([rankedSimilarityAvg.index(Id),rankedReciprocity.index(Id),rankedUrlAvg.index(Id),rankedtextentropy.index(Id),rankedPerstability.index(Id),rankedcommCentralityNormed.index(Id),rankedcommSize.index(Id)])
            if whichmethod == 'size':
                commRanking[Id] = 1/(1+rankedcommSize.index(Id))
            if whichmethod == 'perstability':
                commRanking[Id] = 1/(1+rankedPerstability.index(Id))
            if whichmethod == 'centrality':
                commRanking[Id] = 1/(1+rankedcommCentralityNormed.index(Id))
            if whichmethod == 'diversity':
                commRanking[Id] = 1/(1+rankedtextentropy.index(Id))
                # commRanking[Id] = recRank([rankedUrlAvg.index(Id),rankedtextentropy.index(Id)])
            
        self.rankingDict = rankingDict

        '''All the communities ranked in order of combined importance'''
        rankedCommunities = sorted(commRanking, key=commRanking.get, reverse=True)
        if numTopComms > len(rankedCommunities):
            numTopComms = len(rankedCommunities)

        '''Fix url dictionary'''
        print('Fixing urls...')
        self.commUrlCategory = self.urlDictionaryUpdate(rankedCommunities[0:numTopComms])

        '''Constructing community size heatmap data'''
        commSizeHeatData = np.zeros([numTopComms, timeslots])
        commUrlCategoryHeatmap = {}
        catsPerComm = {}
        for rCIdx, comms in enumerate(rankedCommunities[0:numTopComms]):
            commUrlCategoryHeatmap[rCIdx]={}
            for sizeIdx, timesteps in enumerate(uniCommIdsEvol[comms][0]):
                if commSizeHeatData[rCIdx, timesteps] != 0:
                    commSizeHeatData[rCIdx, timesteps] = np.sum([np.log(uniCommIdsEvol[comms][2][sizeIdx]),commSizeHeatData[rCIdx, timesteps]])
                    if self.commUrlCategory[comms][sizeIdx]:
                        addCategory = self.commUrlCategory[comms][sizeIdx][0]                     
                        if comms in catsPerComm:
                            catsPerComm[comms].append(self.commUrlCategory[comms][sizeIdx][0].lower())
                        else:
                            catsPerComm[comms] = [self.commUrlCategory[comms][sizeIdx][0].lower()]
                        if timesteps in commUrlCategoryHeatmap[rCIdx] and commUrlCategoryHeatmap[rCIdx][timesteps]:
                            commUrlCategoryHeatmap[rCIdx][timesteps] = '\n'.join(list(set([commUrlCategoryHeatmap[rCIdx][timesteps],addCategory])))
                        else:
                            commUrlCategoryHeatmap[rCIdx][timesteps] = addCategory
                else:
                    commSizeHeatData[rCIdx, timesteps] = np.log(uniCommIdsEvol[comms][2][sizeIdx])
                    if self.commUrlCategory[comms][sizeIdx]:
                        commUrlCategoryHeatmap[rCIdx][timesteps] = self.commUrlCategory[comms][sizeIdx][0]
                        if comms in catsPerComm:
                            catsPerComm[comms].append(self.commUrlCategory[comms][sizeIdx][0].lower())
                        else:
                            catsPerComm[comms] = [self.commUrlCategory[comms][sizeIdx][0]]
                        # print('single'+commUrlCategoryHeatmap[rCIdx, timesteps])
        normedHeatdata = commSizeHeatData/commSizeHeatData.max()

        '''Retrieve profile images from usernames'''
        CONS_KEY = 'AvLwOrpwRUQ8lGTNmZmPA'
        CONS_SECRET = '9PxFSwG6DiiAOOCZ5oLHi649gxK3iwf8Q9czNZXFE'
        OAUTH_TOKEN = '1161058188-vlXu5zNTP3SZfubVFWJBMQd4Dq7YBBSYOQPMSyP'
        OAUTH_TOKEN_SECRET = '6sR2NpNGcVkPJsiI1oG0xGKrvssL9O9ARnMycHLV54'
        twitter1 = twython.Twython(CONS_KEY, CONS_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

        CONS_KEY = 'H0gzu5Y4JNKtQ6TwkmyOg'
        CONS_SECRET = 'iLZo1hU7052Nnacj3vRUy974rxastZVzXYuJRKw'
        OAUTH_TOKEN = '545997015-Tl9IQc22jBOBXWxOO0Ysu4oAkzYrN1AkGzBvl4u3'
        OAUTH_TOKEN_SECRET = 'YP4Vng1T4oEHrUODTnMXePIEvidlGtnqAshlu8U2M'
        twitter2 = twython.Twython(CONS_KEY, CONS_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

        CONS_KEY = 'uQIvzF5Staqpn33Yi4SfYA'
        CONS_SECRET = 'i9khJfAMTxu8dLJaGmWCyPkLYDrieUDRxMxyhGWBW8'
        OAUTH_TOKEN = '545997015-AU26dstIdSD5vi0JtV111Z5ZIjNQ2tSs8SBrB3on'
        OAUTH_TOKEN_SECRET = 'vP9OzlHlatuztwvVbQdtytQxcwFrMB6RzbHnY2h0'
        twitter3 = twython.Twython(CONS_KEY, CONS_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

        mytwitter = [twitter1,twitter2,twitter3]
        try:
            self.usernameProfPicDict = pickle.load(open(self.dataset_path + '/data/tmp/usernameProfPicDict.pck', 'rb'))
            print('Old avatar dictionary has '+str(len(self.usernameProfPicDict))+' users; '+str(len(set(list(self.usernameProfPicDict.values()))))+' of which are active.')
            profPicDicKeys = list(self.usernameProfPicDict.keys())
            for k in profPicDicKeys:
                if not self.usernameProfPicDict[k]:
                    del self.usernameProfPicDict[k]
        except:
            self.usernameProfPicDict = {}
            print('New avatar dictionary is created')
            pass
            
        '''Create corpus and stopwords'''
        stop = stopwords.words('english')
        # stop = []
        if self.dirName.startswith('greek'):
            session = requests.Session()
            grstopwords = session.get('https://www.dropbox.com/s/d6rvcmfu6c5jlsp/greek_stopwords.txt?raw=1').content.decode('ISO-8859-7').split('\r\n')
            stop.extend(grstopwords)
        definiteStop = ['gt','amp','rt','via']
        stop.extend(definiteStop)
        if not os.path.exists(self.dataset_path + '/data/tmp/'+self.adaptStr+'/datasetCorpus_prev'+str(prevTimeslots)+self.fileTitle +'.pck'):
            idf,idfBigram = self.corpusExtraction(prevTimeslots)#rankedCommunities[:numTopComms])
        else:
            idf = pickle.load(open(self.dataset_path + '/data/tmp/'+self.adaptStr+'/datasetCorpus_prev'+str(prevTimeslots)+self.fileTitle +'.pck', 'rb'))
            idfBigram = pickle.load(open(self.dataset_path + '/data/tmp/'+self.adaptStr+'/datasetBigramsCorpus_prev'+str(prevTimeslots)+self.fileTitle +'.pck', 'rb'))
            print('loaded word corpus from file')
        if not os.path.exists(self.dataset_path + '/data/tmp/'+self.adaptStr+'/datasetHashtagCorpus_prev'+str(prevTimeslots)+self.fileTitle +'.pck'):
            idfHashtag = self.hashtagCorpusExtraction(prevTimeslots)#rankedCommunities[:numTopComms])
        else:
            idfHashtag = pickle.load(open(self.dataset_path + '/data/tmp/'+self.adaptStr+'/datasetHashtagCorpus_prev'+str(prevTimeslots)+self.fileTitle +'.pck', 'rb'))
            print('loaded hashtag corpus from file')
        #-------------------------
        '''Writing ranked communities to json files + MongoDB'''
        dataset_name=self.dataset_path.split('/')
        dataset_name=dataset_name[-1]+self.adaptStr
        #Mongo--------------------
        # try:
        #     client = MongoClient('160.40.50.236')
        #     db = client[dataset_name]
        #     dyccos=db.dyccos
        # except:
        #     print('mongo client is dead')
        #     pass
        #-------------------------
        jsondata = dict()
        jsondata['ranked_communities'] = []
        jsondata['datasetInfo'] = {'allTimeslots':self.timeLimit,
            'limits':{'min':10000,'max':45000,'usersmin':10,'usersmax':1000,'centmin':1,'centmax':30,'conmin':20,'conmax':200,'fixed':2}}
        '''
        min - distance from left border
        max - distance from right border
        usersmin - min population in comms
        usersmax - max population in comms
        centmin - centrality minimum
        centmax - centrality max
        conmin - min num of connections/edges
        conmax - max num of connections/edges
        fixed - centrality accuracy in digits
        '''

        rankedCommunitiesFinal = {}
        bigramEntropy = {}
        for rank, rcomms in enumerate(rankedCommunities[:numTopComms]):

            tmslUsrsCentral, tmslUsrsProfPics, hashtagList, keywordList, bigramList, tmptweetids, commTwText, urlList, domainList, topic, tmpkeywrds = [], [], [], [], [], [], [], [], [], [], []

            strRank = str(rank)#'{0}'.format(str(rank).zfill(2))
            rankedCommunitiesFinal[strRank] = [rcomms]
            rankedCommunitiesFinal[strRank].append(commRanking[rcomms])
            # rankedCommunitiesFinal[strRank].append(uniCommIdsEvol[rcomms][3])
            timeSlotApp = [self.timeLimit[x] for x in uniCommIdsEvol[rcomms][0]]

            timeStmp_Centrality_Dict = {str(k):0 for k in self.timeLimit}
            communitySizePerSlot = {str(k):0 for k in self.timeLimit}
            communityEdgesPerSlot = {str(k):0 for k in self.timeLimit}            
            communityKeywordsPerSlot = {str(k):[] for k in self.timeLimit}
            communityBigramsPerSlot = {str(k):[] for k in self.timeLimit}
            communityTagsPerSlot = {str(k):[] for k in self.timeLimit}   
            communityUrlsPerSlot = {str(k):[] for k in self.timeLimit}   
            communityDomainsPerSlot = {str(k):[] for k in self.timeLimit}   
            communityTweetsPerSlot = {str(k):[] for k in self.timeLimit}  
            usersCentralityPerSlot = {str(k):[] for k in self.timeLimit}  

            commUserDict = {k:[] for k in range(len(self.timeLimit))}

            print('Building json for dynComm: '+str(rcomms)+' ranked '+str(strRank)+' via value '+str(commRanking[rcomms]))

            for tmsl, users in enumerate(uniCommIdsEvol[rcomms][3]):

                if tmsl>0 and uniCommIdsEvol[rcomms][0][tmsl] == uniCommIdsEvol[rcomms][0][tmsl-1] and uniCommIdsEvol[rcomms][2][tmsl] < uniCommIdsEvol[rcomms][2][tmsl-1]:
                    continue#ensure that the community with the biggest size goes to print...
                timeStmp_Centrality_Dict[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = uniCommIdsEvol[rcomms][5][tmsl]
                communitySizePerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = uniCommIdsEvol[rcomms][2][tmsl]

                '''tmp script for edge computation. normally it would result straight from the extraction def'''
                lines = self.adjListBag[uniCommIdsEvol[rcomms][0][tmsl]]
                tmpNumEdges = 0
                for l in lines:
                    if l[0] in users and l[1] in users:
                        tmpNumEdges += int(l[2])
                communityEdgesPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = tmpNumEdges

                tmpHashtagBag = self.commHashtagBag[rcomms][tmsl]#hashtags for only this slot
                if tmpHashtagBag:
                    tmppopHashtags = [x.lower() for x in tmpHashtagBag]
                    tmppopHashtags = collections.Counter(tmppopHashtags)
                    communityTagsPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = tfidf.comm_tfidf(tmppopHashtags,idfHashtag,10)   
                else:
                    tmppopHashtags = {} 

                hashtagList.append(list(tmppopHashtags.keys()))#hashtags for each slot    

                tmpURLBagAll = [x.rstrip('/').lstrip('http://').lstrip('https://') for x in self.commUrlBag[rcomms][tmsl] if x]#urls for only this slot
                if tmpURLBagAll:
                    # tmppopUrls = [x for x in list(itertools.chain.from_iterable(tmpURLBag))]
                    tmpURLBag = collections.Counter(tmpURLBagAll)
                    communityUrlsPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = tmpURLBag.most_common(10)
                else:
                    tmpURLBag = {}

                tmpDomainBagAll = [urllib.parse.urlparse(x).netloc.lower() for x in self.commUrlBag[rcomms][tmsl] if x]#urls for only this slot
                if tmpDomainBagAll:
                    # tmppopUrls = [x for x in list(itertools.chain.from_iterable(tmpURLBag))]
                    tmpDomainBag = collections.Counter(tmpDomainBagAll)
                    communityDomainsPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = tmpDomainBag.most_common(10)
                else:
                    tmpDomainBag = {}

                # urlList.append(list(tmpURLBag.keys())) # uncomment this to find pop urls over timeslots
                urlList.append(tmpURLBagAll) # uncomment this to find pop urls overall
                domainList.append(tmpDomainBagAll) # uncomment this to find pop urls overall

                commUserDict[uniCommIdsEvol[rcomms][0][tmsl]] = users

                croppedUsers = list(set(users).difference(list(self.usernameProfPicDict.keys())))
                userbatches = [croppedUsers[x:x+100] for x in range(0, len(croppedUsers), 100)] #Retrieve user avatars
                for screenNameList in userbatches:
                    comma_separated_string = ','.join(screenNameList)
                    eror =  '429'
                    while '429' in eror:
                        try:               
                            output = mytwitter[random.randint(0,2)].lookup_user(screen_name=comma_separated_string)
                            for user in output:
                                self.usernameProfPicDict[user['screen_name']] = user['profile_image_url'].replace('_normal','')
                            eror = 'ok'
                        except twython.exceptions.TwythonError as er:
                            eror = str(er)
                            if '429' in eror:
                                print('delaying for batch api...')
                                time.sleep(5*60+2)
                            pass

                uscentr = []
                good = 0      
                for us in users:                
                    if us not in self.usernameProfPicDict or not self.usernameProfPicDict[us]:
                        self.usernameProfPicDict[us] = ''
                    else:
                        good +=1
                    uscentr.append([us, self.userPgRnkBag[uniCommIdsEvol[rcomms][0][tmsl]][us]])      
                uscentr = sorted(uscentr, key=itemgetter(1), reverse=True)
                usersCentralityPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = uscentr[:10]
                tmptweetText = [' '.join([i for i in regex2.findall(regex1.sub('',x.lower())) if i and not i.startswith(('rt','htt','(@','\'@','t.co')) and i not in definiteStop]) for x in self.commTweetBag[rcomms][tmsl]]
                tmptweetText = [x for x in tmptweetText if x]
                seen = set()
                seen_add = seen.add
                tmptweetText2 = [x for x in tmptweetText if x not in seen and not seen_add(x)]
                popTweets = collections.Counter(tmptweetText)
                communityTweetsPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = popTweets.most_common(10)

                #topic extraction
                topicList = ' '.join(tmptweetText)
                topicList = topicList.lower()
                # topicList = regex1.sub('', topicList)
                topicList = regex2.findall(topicList)
                if len(set(topicList)) > 5:
                    for i in list(topicList):
                            if len(i)<=2 or i in stop:
                                del topicList[topicList.index(i)]
                else:
                    for i in list(topicList):
                        if i in definiteStop or not i:
                            del topicList[topicList.index(i)]
                if not topicList:
                    topicList = ['noText','OnlyRefs']
                topicBigrams = list(nltk.bigrams(topicList))
                topicBigrams = [' '.join(x) for x in list(nltk.bigrams(topicList))]

                topicListCounted = collections.Counter(topicList)                
                topicBigramsCounted = collections.Counter(topicBigrams)

                timeSlLen=len(uniCommIdsEvol[Id][0])                
                tmpTopic=tfidf.comm_tfidf(topicListCounted,idf,10)
                tmpBigrams=tfidf.comm_tfidf(topicBigramsCounted,idfBigram,10)
                communityKeywordsPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = tmpTopic                
                communityBigramsPerSlot[str(self.timeLimit[uniCommIdsEvol[rcomms][0][tmsl]])] = tmpBigrams

                keywordList.append(list(topicListCounted.keys()))
                bigramList.append(list(topicBigramsCounted.keys()))

            try:
                hashtagList = list(itertools.chain.from_iterable(hashtagList))
            except:
                pass
            if hashtagList:                
                popHashtags = [x.lower() for x in hashtagList]
                popHashtags = collections.Counter(popHashtags)
                popHashtags=tfidf.comm_tfidf(popHashtags,idfHashtag,10)
                # popHashtags = popHashtags.most_common(10)
            else:
                popHashtags=[]

            if urlList:
                urlList=[x for x in list(itertools.chain.from_iterable(urlList)) if x]
                popUrls = collections.Counter(urlList)
                popUrls = popUrls.most_common(10)
            else:
                popUrls=[]

            if domainList:
                domainList=[x for x in list(itertools.chain.from_iterable(domainList)) if x]
                popDomains = collections.Counter(domainList)
                popDomains = popDomains.most_common(10)
            else:
                popDomains=[]
            # commTweetIds = list(set(tmptweetids))

            try:
                keywordList = list(itertools.chain.from_iterable(keywordList))
            except:
                pass
            if keywordList:                
                # popKeywords = [x.lower() for x in keywordList]
                popKeywords = collections.Counter(keywordList)
                popKeywords=tfidf.comm_tfidf(popKeywords,idf,10)

            try:
                bigramList = list(itertools.chain.from_iterable(bigramList))
            except:
                pass
            if bigramList:                
                popBigrams = [x.lower() for x in bigramList]
                popBigrams = collections.Counter(popBigrams)
                popBigrams=tfidf.comm_tfidf(popBigrams,idfBigram,10)

            dyccoDict = [{
            'timestamp':str(k),
            'commCentrality':timeStmp_Centrality_Dict[str(k)],
            'commSize':communitySizePerSlot[str(k)],
            'commKeywords':communityKeywordsPerSlot[str(k)],
            'connectionsNum':communityEdgesPerSlot[str(k)],
            'communityBigramsPerSlot':communityBigramsPerSlot[str(k)],
            'usersCentrality':usersCentralityPerSlot[str(k)],
            'commHashTags':communityTagsPerSlot[str(k)],
            'commUrls':communityUrlsPerSlot[str(k)],
            'commDomains':communityDomainsPerSlot[str(k)],
            'commTweets':communityTweetsPerSlot[str(k)]} for k in self.timeLimit]

            self.buildDynCommGraphFiles(strRank, commUserDict,prevTimeslots)

            dycco={
            'communityLabels': uniCommIdsEvol[rcomms][8],
            'DyCContainer': dyccoDict,
            'avgDyccoCentrality': rankingDict[rcomms]['commCentralityNormed'], 
            'dyccoPopHashtags': popHashtags,
            'dyccoPopUrls': popUrls,
            'dyccoPopDomains': popDomains,
            'dyccoPopKeywords': popKeywords, 
            'dyccoPopBigrams': popBigrams}

            jsondycco=dycco.copy()
            # dyccos.insert(dycco)
            jsondata['ranked_communities'].append(jsondycco)
        
        twitterDataFile = open(self.dataset_path + '/data/results/'+self.adaptStr+'/'+self.dirName+'communities_prev'+str(prevTimeslots)+self.fileTitle+'.json', 'w')#, encoding='utf-8-sig')
        twitterDataFile.write(json.dumps(jsondata, sort_keys=True))#,ensure_ascii=False).replace('\u200f',''))
        twitterDataFile.close()
        webDrawFile = open('./Com_Graph/web/jsons/'+self.dirName+'communities.json', 'w')
        webDrawFile.write(json.dumps(jsondata, sort_keys=True))
        webDrawFile.close()

        dataEvolPck = open(self.dataset_path + '/data/tmp/'+self.adaptStr+'/dataEvol_prev'+str(prevTimeslots)+self.fileTitle+'.pck', 'wb')
        pickle.dump(self, dataEvolPck, protocol = 2)
        dataEvolPck.close()

        usernameProfPicDictPck = open(self.dataset_path + '/data/tmp/usernameProfPicDict.pck', 'wb') # store the dictionary, for future reference
        pickle.dump(self.usernameProfPicDict, usernameProfPicDictPck, protocol = 2)
        usernameProfPicDictPck.close()

        self.simpleEntropyDict = simpleEntropyDict

        makefigures(whichmethod,commSizeHeatData,self.fileTitle,self.day_month,commRanking,numTopComms,timeslots,uniCommIdsEvol,rankedCommunities,self.commPerTmslt,self.uniCommIds,prevTimeslots,self.dataset_path,self.xLablNum, self.adaptStr,commUrlCategoryHeatmap,catsPerComm,simpleEntropyDict,bigramEntropyDict)
        return rankedCommunitiesFinal


    def buildDynCommGraphFiles(self, strRank, commUserDict,prevTimeslots):    
        # print('Creating a json containing the graphs for dynamic community: '+str(int(strRank)+1))
        '''make and save dynamic community json files'''
        if not os.path.exists(self.dataset_path + '/data/results/' + self.adaptStr +'/partialGraphs/prev'+str(prevTimeslots)+ self.fileTitle):
            os.makedirs(self.dataset_path + '/data/results/' + self.adaptStr +'/partialGraphs/prev'+str(prevTimeslots)+ self.fileTitle)

        allUsers = list(set(itertools.chain.from_iterable(list(commUserDict.values()))))
        allUsers.sort()
        
        allUsernames = []
        for name in allUsers:
            if name in self.tweetDict['userDict']:
                allUsernames.append({'screen_name':name,'avatar':self.usernameProfPicDict[name],'id':self.tweetDict['userDict'][name]['id'],
                    'followers_count':self.tweetDict['userDict'][name]['followers_count'],'listed_count':self.tweetDict['userDict'][name]['listed_count'],
                    'friends_count':self.tweetDict['userDict'][name]['friends_count'],'description':self.tweetDict['userDict'][name]['description'],
                    'name':self.tweetDict['userDict'][name]['name'],'location':self.tweetDict['userDict'][name]['location'],
                    'statuses_count':self.tweetDict['userDict'][name]['statuses_count']})
            else:
                allUsernames.append({'screen_name':name,'avatar':self.usernameProfPicDict[name],'id':'','followers_count':'','listed_count':'','friends_count':'','description':'','name':''})

        jsondata = {'datasetInfo':{'allUsernames':allUsernames},'connections':[]}

        allTmsls = sorted(list(commUserDict.keys()))
        appearingTmsls = [x for x in list(commUserDict.keys()) if commUserDict[x]]
        for tmsl in allTmsls:  
            if tmsl in appearingTmsls:
                lines = self.adjListBag[tmsl]
                tmpConnections = []
                # tmpNumEdges = 0
                for l in lines:
                    if l[0] in commUserDict[tmsl] and l[1] in commUserDict[tmsl] and l[0]!=l[1]:
                        tmpConnections.append(l[0]+';'+l[1]+';'+str(l[2]))
                        # tmpNumEdges += l[2]
                jsondata['connections'].append({'timestamp_connections':tmpConnections})
                # jsondata['edges'].append({'timestamp_connections':tmpNumEdges})  
            else:
                jsondata['connections'].append({'timestamp_connections':[]})
                # jsondata['edges'].append({'timestamp_connections':0})  


        twitterDataFile = open(self.dataset_path + '/data/results/' + self.adaptStr +'/partialGraphs/prev'+str(prevTimeslots)+ self.fileTitle + '/'+self.dirName+'users' + str(int(strRank)+1) +'.json', 'w')#, encoding='utf-8-sig')
        twitterDataFile.write(json.dumps(jsondata, sort_keys=True))#,ensure_ascii=False).replace('\u200f',''))
        twitterDataFile.close()

        webDrawDataFile = open('./Com_Graph/web/jsons/'+self.dirName+'users' + str(int(strRank)+1) +'.json', 'w')
        webDrawDataFile.write(json.dumps(jsondata, sort_keys=True))#,ensure_ascii=False).replace('\u200f',''))
        webDrawDataFile.close()

    def corpusExtraction(self,prevTimeslots):
        from nltk.corpus import stopwords
        from math import log
        import nltk

        print('Extracting dataset corpus')

        stop = stopwords.words('english')
        if self.dirName.startswith('greek'):
            grstopwords=pickle.load(open('./globalDics/greek_stopwords.pck', 'rb'))
            stop.extend(grstopwords)
        stop.extend(['gt','amp','rt','via'])
        stop.sort()
        textList, bigramList = [], []
        # cntr=0
        regex1 = re.compile("(?:\@|#|https?\://)\S+",re.UNICODE)
        regex2 = re.compile("\w+'?\w+",re.UNICODE)

        for k,v in self.commTweetBag.items():
            bagitems = [regex2.findall(regex1.sub('',' '.join(list(set(x))).lower())) for x in v]
            for commWords in bagitems:
                tmpTopicCC = [i for i in commWords if len(i)>2 and not i.startswith(('htt','(@','\'@','t.co')) and i not in stop]
                textList.append(list(set(tmpTopicCC)))
                bigramTopicCC = [' '.join(x) for x in list(nltk.bigrams(tmpTopicCC))]
                bigramList.append(list(set(bigramTopicCC)))
                # print(cntr)
        allWords=list(itertools.chain.from_iterable(textList))
        countAllWords = collections.Counter(allWords)
        allBigrams = list(itertools.chain.from_iterable(bigramList))
        countAllBigrams = collections.Counter(allBigrams)
        dictTokens, dictBigramTokens = {},{}
        textListLength = len(textList)
        for word in set(allWords):
            dictTokens[word]=log(textListLength/(1+countAllWords[word]))
        for bigr in set(allBigrams):
            dictBigramTokens[bigr]=log(textListLength/(1+countAllBigrams[bigr]))

        dictTokensPck = open(self.dataset_path + '/data/tmp/'+self.adaptStr +'/datasetCorpus_prev'+str(prevTimeslots)+ self.fileTitle +'.pck', 'wb') # store the dictionary, for future reference
        pickle.dump(dictTokens, dictTokensPck, protocol = 2)
        dictTokensPck.close()

        dictTokensPck = open(self.dataset_path + '/data/tmp/'+self.adaptStr +'/datasetBigramsCorpus_prev'+str(prevTimeslots)+ self.fileTitle +'.pck', 'wb') # store the dictionary, for future reference
        pickle.dump(dictBigramTokens, dictTokensPck, protocol = 2)
        dictTokensPck.close()

        print('Extracted %s words and %s bigrams' %(len(dictTokens),len(dictBigramTokens)))

        return dictTokens, dictBigramTokens

    def hashtagCorpusExtraction(self,prevTimeslots): 
        from math import log

        print('Extracting hashtag corpus')

        fullList = []
        for k,v in self.commHashtagBag.items():
            listofcomms = [set([y.lower() for y in x if len(y)>2]) for x in v]
            fullList.extend(listofcomms)
                # print(cntr)
        allTags=set(list(itertools.chain.from_iterable(fullList)))
        dictTokens={}
        for word in allTags:
            wordCount=0
            for tmptextlist in fullList:
                if word in tmptextlist:
                    wordCount+=1
            dictTokens[word]=log(len(fullList)/(1+wordCount))

        dictTokensPck = open(self.dataset_path + '/data/tmp/'+self.adaptStr +'/datasetHashtagCorpus_prev'+str(prevTimeslots)+ self.fileTitle +'.pck', 'wb') # store the dictionary, for future reference
        pickle.dump(dictTokens, dictTokensPck, protocol = 2)
        dictTokensPck.close()        
        print('Extracted %s hashtags' %len(dictTokens))
        return dictTokens

    def urlDictionaryUpdate(self,rankedCommunities):
        import urllib.parse
        import  goslate
        from urllib.request import urlopen
        import unshortenCommUrls
        import xml.etree.ElementTree as et

        t=time.time()

        try:
            postsForQueue = pickle.load(open(self.dataset_path + '/data/tmp/commsUrls.pck','rb'))
        except:
            postsForQueue = {}
            pass

        try:
            urlDict = pickle.load(open(self.dataset_path + '/data/tmp/urlDict.pck.pck','rb'))
        except:
            urlDict = {}
            pass

        for Id in rankedCommunities:
            for commUrls in self.commUrlBag[Id]:
                for url in set(commUrls):
                    if url and url not in postsForQueue:
                        if url in urlDict:
                            postsForQueue[url] = {'trueUrl':urlDict[url],'domain':urllib.parse.urlparse(urlDict[url]).netloc.lower()}
                        else:
                            postsForQueue[url] = {'trueUrl':url,'domain':urllib.parse.urlparse(url).netloc.lower()}
        postsForQueue = unshortenCommUrls.unshrinkUrlsInParallel(postsForQueue,self.dataset_path)

        gs = goslate.Goslate()

        #Get shrinked urls
        shrinkedUrls = codecs.open('./globalDics/allShrinks.txt','r','utf-8').readlines()
        shrinkedUrls = [x.strip().lower() for x in shrinkedUrls]
        shrinkedUrls = list(set(shrinkedUrls))
        shrinkedUrls.sort() 

        try:
            domainDict = pickle.load(open('./globalDics/catCommDomDict.pck','rb'))#load domain dictionary
            if '' in domainDict:
                del(domainDict[''])
        except:
            domainDict = {}
            print('no domainDict')
            pass

        try:
            urlCategoryDict = pickle.load(open(self.dataset_path + '/data/tmp/urlCategoryDict.pck','rb'))
        except:
            urlCategoryDict = {}
            print('no urlCategoryDict')
            pass
        try:
            wordTranslator = pickle.load(open('./globalDics/wordTranslator.pck','rb'))
        except:
            wordTranslator = {} 
            print('no wordTranslator')
            pass

        #Make lists of categories from category files
        catfiles = [f[:-4] for f in os.listdir('./url_corpus/categories/') if f.endswith('.txt') and not f.startswith('shrinks')]
        for catnames in catfiles:
            vars()[catnames] = codecs.open('./url_corpus/categories/'+catnames+'.txt','r').readlines()
            # vars()[filed+'Full'] = []
            vars()[catnames] = [x.strip() for x in vars()[catnames]]

        elapsed = time.time() - t
        print('Elapsed: %.2f seconds' % elapsed)
        t=time.time()
        commUrlCategory = {}
        for Id in rankedCommunities:
            commUrlCategory[Id] = []
            for idxC,commUrls in enumerate(self.commUrlBag[Id]):
                commUrls = [x for x in commUrls if x]
                tmpCats = []
                for idxU,url in enumerate(commUrls):
                    try:
                        self.commUrlBag[Id][idxC][idxU] = postsForQueue[url]['trueUrl']
                        trueUrl = postsForQueue[url]['trueUrl']
                        domain = postsForQueue[url]['domain']
                    except:
                        trueUrl = url
                        domain = urllib.parse.urlparse(url).netloc.lower()
                        print(url +' not in dictionary')
                        pass
                    if trueUrl not in urlCategoryDict:
                        for catnames in catfiles:
                            for cat in vars()[catnames]:
                                if cat.lower() in trueUrl.lower():
                                    if trueUrl not in urlCategoryDict:
                                        urlCategoryDict[trueUrl] = [catnames]
                                    else:
                                        urlCategoryDict[trueUrl].append(catnames)
                    if trueUrl not in urlCategoryDict:
                        if domain not in domainDict:
                            try:
                                dataFromDom = urlopen('http://data.alexa.com/data?cli=10&url='+domain).read()
                                data = et.fromstring(dataFromDom)
                                for cat in data.iter('CAT'):
                                    # print (cat.attrib['ID'])
                                    domainDict[domain].extend(cat.attrib['ID'].split('/'))
                                    tmpDom = [x.lower().replace('_',' ').replace('-',' ') for x in list(set(domainDict[domain])) if len(x)>2]
                                    for widx, w in enumerate(tmpDom.copy()):
                                        if w not in wordTranslator:
                                            wordTranslator[w] = gs.translate(w, 'en')
                                        tmpDom[widx] = wordTranslator[w]
                                    domainDict[domain] = tmpDom
                            except:# UnicodeEncodeError:
                                print('errored alexa')
                                pass
                    if domain in domainDict:
                        if trueUrl not in urlCategoryDict:
                            urlCategoryDict[trueUrl] = domainDict[domain]
                        else:
                            urlCategoryDict[trueUrl].extend(domainDict[domain])
                    try:
                        tmpCats.extend(urlCategoryDict[trueUrl])
                    except:
                        pass

                tmpCats = [x for x in tmpCats if x]
                countTmpCats = collections.Counter(tmpCats)
                sortedTmpCats = sorted(countTmpCats, key=lambda k: [countTmpCats[k],len(k)], reverse = True)
                if sortedTmpCats:
                    commUrlCategory[Id].append(sortedTmpCats)
                else:
                    commUrlCategory[Id].append(['none'])

        elapsed = time.time() - t
        print('Elapsed: %.2f seconds' % elapsed)

        urlCategoryDictfile = open(self.dataset_path + '/data/tmp/urlCategoryDict.pck', 'wb')    
        pickle.dump(urlCategoryDict, urlCategoryDictfile, protocol = 2)
        urlCategoryDictfile.close()

        wordTranslatorDictfile = open('./globalDics/wordTranslator.pck', 'wb')    
        pickle.dump(wordTranslator, wordTranslatorDictfile, protocol = 2)
        wordTranslatorDictfile.close()

        domCatPck = open('./globalDics/catCommDomDict.pck','wb')
        pickle.dump(domainDict, domCatPck)
        domCatPck.close()

        return commUrlCategory

def makefigures(whichmethod,commSizeHeatData,fileTitle,day_month,commRanking,numTopComms,timeslots,uniCommIdsEvol,rankedCommunities,commPerTmslt,uniCommIds,prevTimeslots,dataset_path,xLablNum, adaptStr,commUrlCategoryHeatmap,catsPerComm,simpleEntropyDict,bigramEntropyDict):
    print('method selected is: '+whichmethod)
    if not os.path.exists(dataset_path + '/data/results/figs/'+adaptStr):
        os.makedirs(dataset_path + '/data/results/figs/'+adaptStr)
    if not os.path.exists(dataset_path + '/data/tmp/figs/'+adaptStr):
        os.makedirs(dataset_path + '/data/tmp/figs/'+adaptStr)
    '''Label parameters'''
    pertick=int(np.ceil(timeslots/xLablNum))
    if numTopComms>len(rankedCommunities):
        numTopComms=len(rankedCommunities)
    row_labels = day_month#(range(timeslots))
    column_labels = list(range(numTopComms))
    column_labels2 = rankedCommunities[:numTopComms]
    #line styles
    style, color = ['*', '+', 'o','d','h','p'], ['g','r','m','c', 'y', 'k']

    '''Categories per all communities'''
    allCats = list(itertools.chain.from_iterable(list(catsPerComm.values())))
    countCats = collections.Counter(allCats)
    sortCats = sorted(countCats, key=countCats.get, reverse=True)
    for cat in sortCats:
        if countCats[cat] > 1:
            print(cat+'\t'+str(countCats[cat]))
    allCats = list(itertools.chain.from_iterable(list(catsPerComm.values())))
    countCats = collections.Counter(allCats)
    sortCats = sorted(countCats, key=countCats.get, reverse=True)
    sortedVals = sorted(list(countCats.values()), reverse=True)
    # fig6, ax6 = plt.subplots()
    # ax6.stem(sortedVals, 'b-')
    # ax6.set_xticks(range(len(sortedVals)))
    # ax6.set_xticklabels(sortCats,fontsize=7, rotation = 30)
    # for tick in ax6.yaxis.get_major_ticks():
    #     tick.label.set_fontsize(7)
    # xmin, xmax = plt.xlim()
    # plt.xlim( -1, xmax+1 )
    # plt.ylabel('Frequency')
    # plt.xlabel('Categories')
    # plt.tight_layout()
    # fig6 = plt.gcf()
    # plt.draw()
    # fig6.savefig(dataset_path + '/data/results/figs/'+adaptStr +'/categsIn'+str(numTopComms)+'FirstComms' + str(prevTimeslots) + fileTitle + '_' + whichmethod + '.pdf',bbox_inches='tight', format='pdf')
    # plt.close()
    # del(fig6)
    # print('Finished with category frequency fig')

    '''Number of communities/timeslot'''
    fig3, ax3 = plt.subplots()
    ax3.plot(commPerTmslt, 'b-')
    ax3.set_xticks(np.arange(0,len(commPerTmslt),pertick), minor=False)
    ax3.set_xticklabels(row_labels[0::pertick], minor=False, fontsize=7, rotation = 30)
    for tick in ax3.yaxis.get_major_ticks():
        tick.label.set_fontsize(7)
    xmin, xmax = plt.xlim()
    # plt.xlim( 0, xmax+1 )
    plt.ylabel('Community Number Fluctuation')
    plt.xlabel('Timeslots')
    plt.tight_layout()
    fig3 = plt.gcf()
    plt.draw()
    fig3.savefig(dataset_path + '/data/results/figs/'+adaptStr +'/commNumberFlux_prev' + str(prevTimeslots) + fileTitle + '.pdf',bbox_inches='tight', format='pdf')
    plt.close()
    del(fig3)
    print('Finished with number of communities\' fluctuation fig')

    '''Make community size evolution heatmap'''
    fig2, ax = plt.subplots()
    heatmap = ax.pcolormesh(commSizeHeatData, cmap=plt.cm.gist_gray_r)
    ax.set_xticks(np.arange(0,commSizeHeatData.shape[1],pertick), minor=False)
    plt.xlim(xmax=timeslots)
    ax.xaxis.tick_top()
    ax.set_xticklabels(row_labels[0::pertick], minor=False,fontsize=8)
    ax.set_yticks(np.arange(commSizeHeatData.shape[0]), minor=False)
    plt.ylim(ymax=numTopComms)
    ax.invert_yaxis()
    ax.set_yticklabels(column_labels, minor=False, fontsize=7)
    plt.ylabel('Ranked Communities (Best ' + str(numTopComms) + ')')
    ax2 = ax.twinx()
    ax2.set_yticks(np.arange(commSizeHeatData.shape[0]), minor=False)
    plt.ylim(ymax=numTopComms)
    ax2.invert_yaxis()
    ax2.set_yticklabels(column_labels2, minor=False, fontsize=7)
    plt.xlabel('Timeslot', {'verticalalignment': 'top'})
    if numTopComms < 101:
        plt.grid(axis='y')
    fig2 = plt.gcf()
    plt.tight_layout()
    mng = plt.get_current_fig_manager()
    mng.resize(*mng.window.maxsize())
    plt.draw()
    fig2.savefig(dataset_path + '/data/results/figs/'+adaptStr +'/communitySizeHeatmap_prev' + str(prevTimeslots) + fileTitle + '_' + whichmethod + '.pdf',bbox_inches='tight', format='pdf')
    plt.close()
    print('Finished with heat map fig')

    '''Make community size evolution color heatmap'''
    fig5, ax5a = plt.subplots()
    heatmap = ax5a.pcolor(commSizeHeatData,cmap=plt.cm.Blues)
    for y in range(commSizeHeatData.shape[0]):
        for x in range(commSizeHeatData.shape[1]):
            try:
                plt.text(x + 0.5, y + 0.5, '%s' % commUrlCategoryHeatmap[y][x].lower(), horizontalalignment='center',verticalalignment='center',fontsize = 5, rotation = 35)#.decode('utf-8') .encode('utf-8').decode('utf-8-sig')
                # print(commUrlCategoryHeatmap[y][x].lower())
            except KeyError:
                plt.text(x + 0.5, y + 0.5, '',horizontalalignment='center',verticalalignment='center',fontsize = 5, rotation = 35)
                pass
    # plt.colorbar(heatmap)
    ax5a.set_xticks(np.arange(0,commSizeHeatData.shape[1],pertick), minor=False)
    plt.xlim(xmax=timeslots)
    ax5a.xaxis.tick_top()
    ax5a.set_xticklabels(row_labels[0::pertick], minor=False,fontsize=8)
    ax5a.set_yticks(np.arange(commSizeHeatData.shape[0]), minor=False)
    plt.ylim(ymax=numTopComms)
    ax5a.invert_yaxis()
    ax5a.set_yticklabels(column_labels, minor=False, fontsize=7)
    plt.ylabel('Ranked Communities (Best ' + str(numTopComms) + ')')
    ax5b = ax5a.twinx()
    ax5b.set_yticks(np.arange(commSizeHeatData.shape[0]), minor=False)
    plt.ylim(ymax=numTopComms)
    ax5b.invert_yaxis()
    ax5b.set_yticklabels(column_labels2, minor=False, fontsize=7)
    plt.xlabel('Timeslot', {'verticalalignment': 'top'})
    if numTopComms < 101:
        plt.grid(axis='y')
    fig5 = plt.gcf()
    plt.tight_layout()
    mng = plt.get_current_fig_manager()
    mng.resize(*mng.window.maxsize())
    plt.draw()
    fig5.savefig(dataset_path + '/data/results/figs/'+adaptStr +'/communitySizeColorHeatmap_prev' + str(prevTimeslots) + fileTitle + '_' + whichmethod + '.pdf',bbox_inches='tight', format='pdf')
    plt.close()
    print('Finished with heat colormap fig')

    '''Text entropy flux'''
    font = {'size': 12}
    plt.rc('font', **font)
    fig6, ax6 = plt.subplots()
    colormap = plt.cm.gist_ncar
    plt.gca().set_color_cycle([colormap(i) for i in np.linspace(0, 0.9, len(rankedCommunities[:numTopComms]))])
    entropySum = [0]*timeslots
    for Id in rankedCommunities[:numTopComms]:
        commEntropy = [0]*timeslots
        myxaxis=[]
        for idx,timesteps in enumerate(uniCommIdsEvol[Id][0]):
            if not commEntropy[timesteps]:
                commEntropy[timesteps] = bigramEntropyDict[Id][idx]+0.000001
                myxaxis.append(timesteps)
            else:
                commEntropy[timesteps] = max((bigramEntropyDict[Id][idx]),commEntropy[timesteps])
        commEntropyNew=[]
        for x in commEntropy:
            if x:
                commEntropyNew.append(x)
        entropySum = [x + y for x, y in zip(entropySum, commEntropy)]
        # print(Id)
        # print(myxaxis)
        # print(commEntropyNew)
        plt.plot(myxaxis,commEntropyNew)#, hold=True)
    ax6.set_xticks(np.arange(0,len(commPerTmslt),pertick), minor=False)
    ax6.set_xticklabels(row_labels[0::pertick], minor=False, fontsize=12, rotation = 30)
    plt.ylabel('Community text entropy')
    plt.xlabel('Timeslots')
    plt.tight_layout()
    fig6 = plt.gcf()
    plt.draw()
    fig6.savefig(dataset_path + '/data/results/figs/'+adaptStr +'/commEntropyFlux_prev' + str(prevTimeslots) + fileTitle + '_' + whichmethod + '.pdf',bbox_inches='tight', format='pdf')
    plt.close()
    del(fig6)
    print('Finished with community entropy fluctuation fig')

    '''sum of entropies   '''
    methodfiles = [f[:-4] for f in os.listdir(dataset_path + '/data/tmp/figs/'+adaptStr) if f.endswith('.pck')]

    entropySum =  [x/numTopComms for x in entropySum]
    font = {'size': 12}
    plt.rc('font', **font)
    fig7, ax7 = plt.subplots()    
    ymax = 0
    sumofSumsofEntropy = {}
    for styleIdx, methodname in enumerate(methodfiles):
        if methodname == whichmethod:
            continue
        oldentropy = pickle.load(open(dataset_path + '/data/tmp/figs/'+adaptStr +'/' + methodname + '.pck','rb'))
        plt.plot(oldentropy,linestyle='None', marker=style[styleIdx], color=color[styleIdx], label = methodname)  #linestyles[styleIdx]
        ymax = max([ymax,max(oldentropy)])
        sumofSumsofEntropy[methodname] = sum(oldentropy)
    plt.plot(entropySum,linestyle='None', marker=r'$\bowtie$', color='b', label = whichmethod)    
    plt.ylim(ymax = int(np.ceil(max([max(entropySum),ymax]))))
    plt.xlim(xmin =  -1 , xmax = len(entropySum))
    ax7.set_xticks(np.arange(0,len(commPerTmslt),pertick), minor=False)
    ax7.set_xticklabels(row_labels[0::pertick], minor=False, fontsize=12, rotation = 30)
    plt.ylabel('Sum of entropy')
    plt.xlabel('Timeslots')
    plt.legend()
    plt.tight_layout()
    fig7 = plt.gcf()
    plt.draw()
    fig7.savefig(dataset_path + '/data/results/figs/'+adaptStr +'/dyCCoEntropyFlux_prev' + str(prevTimeslots) + fileTitle + '.pdf',bbox_inches='tight', format='pdf')
    plt.close()
    del(fig7)
    pickle.dump(entropySum, open(dataset_path + '/data/tmp/figs/'+adaptStr +'/' + whichmethod + '.pck', 'wb'), protocol = 2)
    sumofSumsofEntropy[whichmethod] = sum(entropySum)
    for k in sumofSumsofEntropy.keys():
        print(k + "'s sum of entropy is: "+str(sumofSumsofEntropy[k]))


def product(mylist):
    p = 1
    for i in mylist:
        p *= i
    return p

def recRank(mylist):#Perform the Reciprocal Rank Fusion for a list of rank values
    finscore = []
    mylist=[x+1 for x in mylist]
    for rank in mylist:
        finscore.append(1/(rank))
    return sum(finscore)

def intersectComms(clmns2, prevComms, tempcommSize, bag1, thres):
    # clmns2, prevComms = clmns2prevComms[0], clmns2prevComms[1]
    # print('tempcommSize '+str(tempcommSize)+'\n')
    # print('bag1 '+','.join([str(x) for x in bag1])+'\n')
    # print('clmns2 '+str(clmns2)+'\n')
    # print('prevComms '+','.join([str(x) for x in prevComms])+'\n')
    # print('thres '+str(thres)+'\n')
    # time.sleep(60)
    if thres > (len(prevComms) / tempcommSize) or thres > (tempcommSize / len(prevComms)):
        interResult = False
    else:
        sim = len(set(bag1).intersection(prevComms)) / len(set(np.append(bag1, prevComms)))
        if sim >= thres:
            interResult = sim
        else:
            interResult = False
    return clmns2, interResult

def rankdata(a):
    n = len(a)
    ivec=sorted(range(len(a)), key=a.__getitem__)
    svec=[a[rank] for rank in ivec]
    sumranks = 0
    dupcount = 0
    newarray = [0]*n
    for i in range(n):
        sumranks += i
        dupcount += 1
        if i==n-1 or svec[i] != svec[i+1]:
            averank = sumranks / (dupcount) + 1
            for j in range(i-dupcount+1,i+1):
                newarray[ivec[j]] = averank
            sumranks = 0
            dupcount = 0
    return newarray

def myentropy(data):
    if not data:
        return 0
    entropy = 0
    for x in set(data):
        p_x = float(data.count(x))/len(data)
        if p_x > 0:
            entropy += -p_x*math.log(p_x, 2)
    return entropy

def myselection(url,dataList):
    datadict={}
    for idx,i in enumerate(dataList):
        datadict[idx] = i
    print(url)
    print(datadict)
    myinput = int(input('Which should i select?'))
    return datadict[myinput]