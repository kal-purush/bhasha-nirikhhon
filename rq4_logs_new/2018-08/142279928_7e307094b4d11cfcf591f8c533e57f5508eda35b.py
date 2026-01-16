import sys
import re
from utils import Utilities
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates
import matplotlib
import pandas as pd
import csv
import os
import plotComments


reload(sys)
sys.setdefaultencoding('utf-8')

class Analyzer(object):
    def __init__(self, group):
        self.group = group
        self.display = (self.group == "event_id")

        # Load the positive and negative words
        self.words = {}
        with open("words/positive.txt") as file:
            for line in file:
                self.words[line.rstrip()] = 1
        with open("words/negative.txt") as file:
            for line in file:
                self.words[line.rstrip()] = -1

    def analyze(self, message):
        score = 0
        found = 0
        disp = ""

        i = 0
        # try:
        parts = Utilities.split(message)
        # except AttributeError as e:
        #     print message #None

        for w in parts:
            if w in self.words:
                score += self.words[w]
                found += 1
                if self.display:
                    i = message.lower().find(w, i)
                    d = Utilities.get_colored_text(self.words[w], message[i:i+len(w)])
                    message = message[:i] + d + message[i+len(w):]
                    i = i + len(d)

                    disp += d + " "

        label = score / float(found) if found != 0 else 0.0
        return (label, disp, message)

    # def output(self, group, message, label, disp):
    def output(self, name, repo, group, message, label, disp, time):
        # f = open('result.txt','a')
        with open(name,"a+") as csvfile:
            writer = csv.writer(csvfile)
            # writer.writerow([repo,label,time]) #["repo","score","time"]
            writer.writerow([str(repo),label,1]) #["repo","score","time"]

        # r = "{}\t".format(repo)
        # g = "{}\t".format(group) if self.group != "score" else ""

        # text = ""
        # if self.display:
            # text = "\t{}| {}".format(disp, message.replace('\n',' '))

        # print("{}{:.2f}{}".format(g, label, text))
        # print("{}{}{:.2f}{}\t{}".format(r, g, label, text, time))

        # print("{}{}{:.2f}{}".format(r, g, label, text))
        # print ('\n')

        # f.write("{}\t{:.2f}".format(time,label))
        # f.write('\n')
        # f.close()

    def reprocess(self, name):
        dataframe = pd.read_csv(name)
        dataframe = dataframe.groupby('repo').sum()

        new_name = name[:-4] + '_pcsed.csv'
        dataframe.to_csv(new_name,index=True,sep=',')

    # plot function need to be rewritten using csv
    def plot(self):
        f = open('result.txt','r')

        plt.figure(figsize=(20,6))
        dates = []
        Y1 = []
        for line in f.readlines():
            dates.append(line.split('\t')[0])
            Y1.append(float(line.split('\t')[1]))

        f.close()
        X=range(len(dates))

        # plt.bar(X, Y1, width = 0.35,facecolor = 'lightskyblue',edgecolor = 'white')
        plt.plot(X, Y1, 'co-')
        # plt.bar(X,Y1,width = 0.35,facecolor = 'lightskyblue',edgecolor = 'white')
        plt.xticks(X,dates,rotation=25)
        plt.margins(0.08)
        plt.subplots_adjust(bottom=0.15)
        plt.xlabel('time')
        plt.ylabel('rate')

        plt.show()

def generate_average(ifname, ofname, index):
    with open(ifname,'r') as csvfile :
        reader = csv.reader(csvfile)
        with open(ofname,'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([index, "average"])

        for line in reader:
            if line[0] == index: continue ;
            var = line[0]
            ave = float(line[1])/int(line[2])
            ave = round(ave,2)

            with open(ofname,"a+") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([var, ave]) #["repo","score","time"]

def main(argv):
    # group = argv[0] if len(argv) > 0 else "event_id"
    path = argv[0]
    year = path[-4:]
    # name = year + 'repo.csv' #2017repo.csv

    group = "event_id"
    analyzer = Analyzer("event_id")


    # for data in Utilities.read_json(sys.stdin, group=group):
    for dir in os.listdir(path):
        name = 'tmp/'+dir+'.csv' #2017-01.csv
        new_name = 'tmp2/'+dir+'.csv' #2017-01.csv
        newnew_name='Result/'+dir+'.csv'

        with open(name,"w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["repo","score","num"])
        dir_name = path + '/' + dir

        if dir == '.DS_Store': continue;
        if os.path.isdir(dir_name)== False: continue;

        for filename in os.listdir(dir_name):
            if filename[-5:]!='.json': continue;
            fullname = dir_name+'/'+filename
            print "processing...", fullname
            f = open(fullname,'r')

            for data in Utilities.read_json(f, group=group):

                # data['message'] -- text, data['group'] -- id
                # print data
                if data['message'] == None: continue;

                (label, disp, message) = analyzer.analyze(data["message"])
                group = data["group"] if "group" in data else ""

                raw_time = data['time']
                time = re.findall(r"(.+?)T",raw_time)[0]+' '+re.findall(r"T(.+?)Z",raw_time)[0]

                repo = data['repo']

                analyzer.output(name, repo, group, message, label, disp, time)
                # analyzer.output(group, message, label, disp)
        dataframe = pd.read_csv(name)
        dataframe = dataframe.groupby('repo').sum()
        dataframe.to_csv(new_name,index=True,sep=',')
        # generate_average(new_name,newnew_name,"repo")
    # analyzer.reprocess(name)

def analyzeComments(repo):
    main(repo)
    plotComments.plotComments(repo)