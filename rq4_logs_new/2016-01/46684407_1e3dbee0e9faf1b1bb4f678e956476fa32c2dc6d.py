#!/usr/bin/python
'''
Process list of nominations and 
save diagnostics of interest to csv in convenient form for d3.js plot
'''
#Remember original point was to see if people who've won in an
#  early season become less likely to win in later seasons

#**manually add tie cases where noms[1] also wins: Actor Drama 76, Actress Comedy 74,
#  Actor Comedy 78, Show Comedy 80, Actress Drama 82, Actress Comedy 86, 
#check 88,89,91,95,99

import csv,re,time
import numpy as np
import matplotlib.pyplot as plt

saveNames = ['Actor Drama','Actor Comedy','Actress Drama',
            'Actress Comedy','Show Drama','Show Comedy']              
hits = {'ActorDrama':0,'ActorComedy':0,'ActressDrama':0,
        'ActressComedy':0,'SeriesDrama':0,'SeriesComedy':0}

showNoms = {}; showWins = {}
personNoms = {}; personWins = {}

winWithPrevNoms = dict(zip(range(15),[0]*15))
loseWithPrevNoms = dict(zip(range(15),[0]*15))
winWithPrevWins = dict(zip(range(10),[0]*10))
loseWithPrevWins = dict(zip(range(10),[0]*10))
winBySeason = dict(zip(range(1,16),[0]*15))
loseBySeason = dict(zip(range(1,16),[0]*15))
winByRunPt = dict()
loseByRunPt = dict()

showStartDates = {}; showEndDates = {}

def updateHistories(noms,prevNoms,winWithPrevNoms,loseWithPrevNoms):
    if noms[0] in prevNoms.keys():
        winWithPrevNoms[prevNoms[noms[0]]] += 1
    else:
        winWithPrevNoms[0] += 1
    for p in noms[1:]:
        if p in prevNoms.keys():
            loseWithPrevNoms[prevNoms[p]] += 1
        else:
            loseWithPrevNoms[0] += 1
    
    return winWithPrevNoms,loseWithPrevNoms

def updateNoms(noms,prevNoms,prevWins):
    for p in noms:
        if p in prevNoms.keys():
            prevNoms[p] += 1
        else:
            prevNoms[p] = 1
    if noms[0] in prevWins.keys():
        prevWins[noms[0]] += 1
    else:
        prevWins[noms[0]] = 1

#-----

with open('gglobesShowStartDates.json','r') as f:
    showStartDates = json.load(f)
with open('gglobesShowEndDates.json','r') as f:
    showEndDates = json.load(f)

years = []
with open('gglobesRawData.csv','r') as csvfile:
    myreader = csv.reader(csvfile)
    count = 0
    nextAreNoms = False
    for row in myreader:
        try:
            if 1960 < int(row) < 2020:
                count = 0
                noms = []
        except:
            pass

        if count == 0:
            years.append(int(row[0]))
            print row[0]
        elif row[0] in saveNames:
            nextAreNoms = True
            noms = []
        else:
            if len(row) == 1:
                noms.append(row[0])
            else:
                noms.append((row[0],row[1]))

        if len(noms) == 5:
            print noms

            if type(noms[0]) == type(''):
                #record wins and losses as fn of previous total noms
                winWithPrevNoms,loseWithPrevNoms = \
                    updateHistories(noms,showNoms,
                        winWithPrevNoms,loseWithPrevNoms)

                #record wins and losses as fn of previous total noms
                winWithPrevWins,loseWithPrevWins = \
                    updateHistories(noms,showWins,
                        winWithPrevWins,loseWithPrevWins)

                #update noms and wins
                updateNoms(noms,showNoms,showWins)
            else:
                #record wins and losses as fn of previous total noms
                winWithPrevNoms,loseWithPrevNoms = \
                    updateHistories(noms,personNoms,
                        winWithPrevNoms,loseWithPrevNoms)

                #record wins and losses as fn of previous total noms
                winWithPrevWins,loseWithPrevWins = \
                    updateHistories(noms,personWins,
                        winWithPrevWins,loseWithPrevWins)

                updateNoms(noms,personNoms,personWins)

            noms = []


        count += 1

time.sleep(100)


#**move this into year loop

#record wins and losses as fn of season
thisSeason = year - showStartDates[noms[0]]
winBySeason[thisSeason] += 1
for s in noms[1:]:
    thisSeason = year - showStartDates[s]
    loseBySeason[thisSeason] += 1

#and as fn of point in total run
pointInRun = (year-showStartDates[noms[0]])/\
    float(showEndDates[noms[0]]-showStartDates[noms[0]]+1)
if round(pointInRun,3) not in winByRunPt.keys():
    winByRunPt[round(pointInRun,3)] = 1
else:
    winByRunPt[round(pointInRun,3)] += 1
for s in noms[1:]:
    pointInRun = (year-showStartDates[s])/\
        float(showEndDates[s]-showStartDates[s]+1)
    if round(pointInRun,3) not in loseByRunPt.keys():
        loseByRunPt[round(pointInRun,3)] = 1
    else:
        loseByRunPt[round(pointInRun,3)] += 1







#Can plot histogram of winWithPrevNoms, but should be weighted somehow
#  by most common number of prevNoms, which is winWithPrevNoms+loseWithPrevNoms
#Or maybe it should be histogram of winWithPrevNoms/(totalWithPrevNoms)
prevNomsOrWins = range(15)
winFracPrevWins = [0]*10
winFracPrevNoms = [0]*15
winFracBySeason = [0]*15
for i in range(10):
    if winWithPrevWins[i]+loseWithPrevWins[i] > 0:
        winFracPrevWins[i] = winWithPrevWins[i] / \
            float(winWithPrevWins[i]+loseWithPrevWins[i])
    else:
        winFracPrevWins[i] = np.nan

for i in range(15):
    if winWithPrevNoms[i]+loseWithPrevNoms[i] > 0:
        winFracPrevNoms[i] = winWithPrevNoms[i] / \
            float(winWithPrevNoms[i]+loseWithPrevNoms[i])
    else:
        winFracPrevNoms[i] = np.nan

for i in range(1,13):
    if winBySeason[i]+loseBySeason[i] > 0:
        winFracBySeason[i] = winBySeason[i] / \
            float(winBySeason[i]+loseBySeason[i])
    else:
        winFracBySeason[i] = np.nan

plt.plot(prevNomsOrWins,winFracPrevNoms,'ks',ms=10)
plt.xlim([-0.5,9.5])
plt.xlabel('Previous Noms'); plt.ylabel('Win Fraction')
plt.show()
#may rather count previous losing noms

plt.plot(prevNomsOrWins[:10],winFracPrevWins,'ks',ms=10)
plt.xlim([-0.5,9.5])
plt.xlabel('Previous Wins'); plt.ylabel('Win Fraction')
plt.show()

#Also to do by show season, scraped from IMDB.
#Instead of scaling by total noms for that season, scale
#  by point in the show's run?
plt.plot(range(1,16),winBySeason.values(),'ks',ms=10)
plt.xlim([0.5,12.5])
plt.xlabel('Season'); plt.ylabel('Win Fraction')
plt.show()


#write to file
if 0 and len(years) >= 3 and \
    [hits.values()[i] == len(years) for i in range(6)]:
    with open('%i-%i_winFracPrevNoms.csv' % (years[0],years[-1]),
        'w') as csvfile:
        mywriter = csv.writer(csvfile)
        mywriter.writerow(['prevNoms','numWins','numTot','winFrac'])
        for i in range(len(winFracPrevNoms)):
            mywriter.writerow([prevNomsOrWins[i],winWithPrevNoms[i],
                winWithPrevNoms[i]+loseWithPrevNoms[i],
                winFracPrevNoms[i]])

    with open('%i-%i_winFracPrevWins.csv' % (years[0],years[-1]),
        'w') as csvfile:
        mywriter = csv.writer(csvfile)
        mywriter.writerow(['prevWins','numWins','numTot','winFrac'])
        for i in range(len(winFracPrevWins)):
            mywriter.writerow([prevNomsOrWins[i],winWithPrevWins[i],
                winWithPrevWins[i]+loseWithPrevWins[i],
                winFracPrevWins[i]])

    with open('%i-%i_winBySeason.csv' % (years[0],years[-1]),
        'w') as csvfile:
        mywriter = csv.writer(csvfile)
        mywriter.writerow(['season','numWins','numTot','winFrac'])
        for i in range(len(winBySeason)):
            mywriter.writerow([i+1,winBySeason[i+1],
                winBySeason[i+1]+loseBySeason[i+1],
                winFracBySeason[i]])

