# this module is responsible for loading user movement data
# and parsing into appropriate chunks and structures on demand

import math
import datetime as dt
import pprint
PP = pprint.PrettyPrinter(indent=2)

# bring all the sources together
# input is of the format:
# { uid: {'layouts' -> [ (x,y) ],
#         'terms' -> [ term ],
#         'logs' -> [ (log tuple) ], *(dt, marker, [info])
#         'observations' -> { 'comments': [ comments at end ],
#                             'gender': 'f' or 'm',
#                             'job': str,
#                             'insight_names': [str]
#                             'starttime': dt
#                             'insights': [ { 'insights': { isightname: num },
#                                             'notes': str,
#                                             'time': dt } ]
#                           }
#        }
# }
# bring all the sources together and create line and dots
# that will be shown in the visualization
def aggregateData(fulldata):
    if len(fulldata) == 0:
        raise ValueError("Called aggregate data without data!")
    
    # get documents set
    docs = [ "doc" + str(i) for i in range(50) ]
    docs.extend(fulldata[fulldata.keys()[0]]['terms'])

    # get the info about the useful distance function in the data
    dUsrResult = {}
    for uid in fulldata:
        print "processing user: " + str(uid)
        DFInfo = distanceFunctions(fulldata[uid], docs)
        DFInfo = addInitDot(DFInfo, fulldata[uid]['initLayoutPoint'])
        LineInfo = lines(fulldata[uid], DFInfo, docs)

        LineInfo = lastLineToDot(DFInfo, LineInfo)
        dUsrResult[uid] = { 'DFs': DFInfo, 'lines': LineInfo }

    # flip the result from user->{df->_, lines->} to df->users, lines->users
    #dResult = {'DFs': {}, 'lines': {}}
    #for uid in dUsrResult:
    #    dResult['DFs'][uid] = dUsrResult[uid]['DFs']
    #    dResult['lines'][uid] = dUsrResult[uid]['lines']

        
    
    return dUsrResult


# takes user dict of full info and returns a list of all the observations
def observationSet(fulldata):
    return []
    
    

# get the list of distance functions based on the full range of
# collected data for a given user
# * return format is [ (time, (layoutx,y), info_string ) ]
def distanceFunctions(userdata, docs):
    lLayouts = userdata['layouts']

    # go through logs and get pairs of GO and DF entries
    GOnDFPairs = [] # of tuples ( GO entry, DF entry )
    bGotGO = False
    currGO = ""
    for entry in userdata['logs']:
        if "GO" in entry[1]:
            currGO = entry
            bGotGO = True
        elif "DF" in entry[1]:
            if not bGotGO:
                raise ValueError("DF without GO!")
            GOnDFPairs.append( (currGO, entry) )
            bGotGO = False
        elif bGotGO: # looking for DF but not on (no DF after GO)
            print "..no DF after GO"
            bGotGO = False
            #raise ValueError("no DF after GO")

    if len(lLayouts) != len(GOnDFPairs):
        raise ValueError("different number of layouts and GOnDF pairs")

    starttime = userdata['observations']['starttime']    

    # create set of DF info (a la return val) for all observed DFs
    DFInfo = [ (deltaSeconds(starttime, pair[1][0]),
                lLayouts[i],
                "<br>DF Number: " + str(i+1) + " <br /><br> <u>Top 10 Keywords</u>: <br>" +
                  best10TermsText(pair[1][2], docs) )
               for i, pair in enumerate(GOnDFPairs) ]

    print "..DFS pre filter:"
    PP.pprint( [ (x[0], x[1]) for x in DFInfo ])

    # filter the results based on observed start time
    DFInfo = filter(lambda info: info[0] >= 0, DFInfo)

    print "..DFS post filter:"
    PP.pprint( [ (x[0], x[1]) for x in DFInfo ])

    return DFInfo


# get the list of lines needed based on the full range of
# collected data for a given user
# * return format is [ {x1:_, y1:_, x2:_, y2:_, info:_ } ]
readDeltaT = 3
def lines(userdata, DFInfo, docs):
    lLayouts =  [ x[1] for x in DFInfo ]  #userdata['layouts']
    print "lLayouts", lLayouts
    starttime = userdata['observations']['starttime']    

    # go through logs to get all the DFs and collect what
    # comes between them
    lLineInfo = []
    def newLineInfo():
        return { 'read': set([]), 'undo': False, 'reset': False,
                 'search': [], 'GOs': [], 'obs': [],
                 'tStart': 0, 'tEnd': 0}
    lineInfo = newLineInfo()
    lineInfo['tStart'] = starttime
    lastTime = 0
    bSeenUndo = True # NB only allowed one undo per DF, not after reset
    bSeenReset = True # same with reset
    bPastStarttime = False
    addRead = None
    for iEntry, entry in enumerate(userdata['logs']):
        # use this bool gate instead of simple time<starttime filter
        # because user1's data has a big break in it that causes probs
        #if not bPastStarttime:
        #    if entry[0] >= starttime:
        #        bPastStarttime = True
        #    print (entry[0], entry[1])
        #    continue
        if entry[0] < starttime:
        #    #print (entry[0], entry[1])
            continue

        if 'DF' in entry[1]:
            if lineInfo['tEnd'] == 0:
                lineInfo['tEnd'] = lastTime
            lLineInfo.append(lineInfo)
            lineInfo = newLineInfo()
            lineInfo['tStart'] = entry[0]
            bSeenUndo = False
            bSeenReset = False

        elif 'DOC' in entry[1]:
            # create a lookahead to possibly add
            addRead = (entry[0], entry[2], entry[1])

        elif 'UNDO' in entry[1]:
            if not bSeenUndo and not bSeenReset:
                lineInfo['undo'] = True
                lineInfo['tEnd'] = lastTime
                lLineInfo.append(lineInfo)
                lineInfo = newLineInfo()
                lineInfo['tStart'] = entry[0]
                bSeenUndo = True
        
        elif 'RESET' in entry[1]:
            if not bSeenReset:
                lineInfo['reset'] = True
                lineInfo['tEnd'] = lastTime
                lLineInfo.append(lineInfo)
                lineInfo = newLineInfo()
                lineInfo['tStart'] = entry[0]
                bSeenReset = True
                bSeenUndo = True
            
        elif 'SEARCH' == entry[1]: # avoid SEARCH_HILITE
            lineInfo['search'].append(entry[2])
            
        elif 'GO1' in entry[1]:
            newEntry = {}
            newEntry['terms1'] = termsList([ int(e[0]) for e in entry[2][0] ],
                                           userdata['terms'])
            lineInfo['GOs'].append(newEntry)

        elif 'GO2' in entry[1]:
            newEntry = {}
            newEntry['terms1'] = termsList([ int(e[0]) for e in entry[2][0][0] ],
                                           userdata['terms'])
            newEntry['terms2'] = termsList([ int(e[0]) for e in entry[2][0][1] ],
                                           userdata['terms'])
            lineInfo['GOs'].append(newEntry)

        if 'GO' in entry[1]: # set end time (gets reset next go if no DF after)
            lineInfo['tEnd'] = entry[0]

        # look back to see if last DOC entry should be added
        if addRead:
            if not 'MOUSE' in addRead[2]:
                lineInfo['read'] = lineInfo['read'].union(addRead[1])
            else:
                if lastTime != 0 and \
                   timeDuration(lastTime, addRead[0]).seconds >= readDeltaT:
                    lineInfo['read'] = lineInfo['read'].union(addRead[1])
            
            addRead = None

        lastTime = entry[0]

    # create the very last line entry from whatever
    # happened with the last DF
    print "setting lineinfo ", lineInfo['tEnd'] , " to ", lastTime
    lineInfo['tEnd'] = lastTime
    lLineInfo.append(lineInfo)

    # for each line (that will be in vis), get observations in the
    # interval and append their info to the data
    for line in lLineInfo:
        setObs = set([])
        print ".. processing line: ", line['tStart'], " to ", line['tEnd']
        for iEntry, entry in enumerate(userdata['observations']['insights']):
            if entry['time'] >= line['tStart'] and \
               entry['time'] <= line['tEnd']:
                setObs = setObs.union(entry['insights'].keys())
	line['obs'] = list(setObs)

    # create the actual line info format for the web
    idxStartLayout = 0
    idxEndLayout = 1
    idxLastStart = 0
    visLines = []
    def textLineInfo(line):
	duration = timeDuration(line['tStart'], line['tEnd'])
        return "<u>From</u> " + str(line['tStart']) + " <u>for</u> " + str(duration) + "<br />" + \
	       "<u>Read</u>: " + termsList(line['read'], userdata['terms']) + \
               "<br />" + \
               "<u>Searches</u>: " + str(line['search']) + "<br />" + \
               "<u>GOs</u>: " + str(line['GOs']) + "<br />" + \
               "<u>obs</u>: " + str(line['obs'])
    for iLine, line in enumerate(lLineInfo[:-1]): #not last one; it goes nowhere
        info = textLineInfo(line)
        lineStart = lLayouts[idxStartLayout]
        lineEnd = ()
        if line['undo']:
	    lineEnd = lLayouts[idxLastStart]
            idxStartLayout = idxLastStart
        elif line['reset']:
	    lineEnd = lLayouts[0]
            idxStartLayout = 0
	    idxLastStart = 0
	else:
	    lineEnd = lLayouts[idxEndLayout]
	    idxLastStart = idxStartLayout
	    idxStartLayout = idxEndLayout
	    idxEndLayout += 1

        visLines.append( {
            'x1': lineStart[0],
            'y1': lineStart[1],
            'x2': lineEnd[0],
            'y2': lineEnd[1],
            'info': info,
            'count': len(info.split('<br />')[1].split(',')), ##Sriram: added this to also include count as one of the features
            'backward': line['reset'] or line['undo']} )

    # add the last line as a phantom line; it represents what happened
    # after the last DF and will get sucked up into the last dot anyway
    visLines.append( {'x1':0, 'y1':0, 'x2':0, 'y2':0,
                      'info': textLineInfo(lLineInfo[-1]),
                        'count': len(info.split('<br />')[1].split(',')), ##Sriram: added this to also include count as one of the features
                      'backward': False } )

    return visLines


# subtract two datetime.time objects by first making dummy
# datetime.datetime objects (whic can be subtracted)
# * return type is a datetime.timedelta
def timeDuration(t1, t2):
    dt1 = dt.datetime(1,1,1,1,1,1)
    dt2 = dt.datetime(1,1,1,1,1,1)
    dt1 = dt1.replace(hour=t1.hour, minute=t1.minute, second=t1.second)
    dt2 = dt2.replace(hour=t2.hour, minute=t2.minute, second=t2.second)
    return dt2 - dt1


# the last line goes to nowhere, so instead we put its info
# on the info of the last distance function
def lastLineToDot(DFInfo, LineInfo):
    lastDFI = DFInfo[-1]
    DFInfo[-1] = (lastDFI[0], lastDFI[1],
                  lastDFI[2] + "<hr>" + LineInfo[-1]['info'] )
    if not (LineInfo[-1]['x2'] == 0 and LineInfo[-1]['y2'] == 0):
        print "ALERT: NOT LAST 0 LINE"
    return LineInfo[:-1]


def addInitDot(DFInfo, initLayoutPt):
    return [ (0, initLayoutPt, "Starting..." ) ] + DFInfo


# get the delta seconds from two timestamps
# (d2 - d1)
def deltaSeconds(time1, time2):
    dt1 = dt.datetime(1, 1, 1,
                      time1.hour, time1.minute, time1.second)
    dt2 = dt.datetime(1, 1, 1,
                      time2.hour, time2.minute, time2.second)

    return (dt2-dt1).total_seconds()
    

# get the best 10 terms based on coeffs from the list
def best10TermsText(L, docs):
    decorated = zip(range(len(L)), map(abs,L))
    decorated.sort(key=lambda x: x[1])
    idxs = (zip(*decorated)[0])[-10:]

    return "<br />".join([ docs[i] for i in idxs ])

def termsList(lIdxs, lTerms):
    return ", ".join([ lTerms[idx] for idx in lIdxs ])