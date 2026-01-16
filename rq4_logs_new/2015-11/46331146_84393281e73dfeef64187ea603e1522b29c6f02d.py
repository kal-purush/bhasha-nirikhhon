import csv
import os
import pandas
import matplotlib.pyplot as plt

#aggregate csv
colnames = ['age','gender','impressions','clicks','signed_in']
data = pandas.read_csv('http://stat.columbia.edu/~rachel/datasets/nyt1.csv',names = colnames)[1:]
url = ["http://stat.columbia.edu/~rachel/datasets/nyt"+str(n)+".csv" for n in range(2,31)]
for n in range(2,31):
    data2 = pandas.read_csv(url[n],names = colnames)[1:]
    data = data.append(data2)
data.to_csv('nytime_aggregation.csv')

#Failed to make CTR
##########################################
#This doesn't work as impressions can be zero. I try to create a new column of CTR
def CTR(x):
    return x['clicks']*x['impressions']
CTR = [float(data.clicks[n])/float(data.impressions[n]) for n in range(len(data.clicks))]
data.apply(CTR)
# this doesn't work for the same reason
[float(data.clicks[n])/float(data.impressions[n]) for n in range(1,40)]
# Can't seem to make this work
for n in range(1,40):
    a = float(data.impressions[n])
    if a!=0:
        float(data.clicks[n])/float(data.impressions[n]
        else:
###########################################

age = data.age.tolist()[1:]
age = map(int,age)
avg_age = sum(age)/float(len(age)-age.count(0))

impressions = data.impressions.tolist()[1:]
impressions = map(int,impressions)
clicks = data.clicks.tolist()[1:]
clicks = map(int,clicks)


print 'average age:',avg_age
print 'average impression per click:',float(sum(impressions)/sum(clicks))
print 'CTR:', float(sum(clicks)/sum(impressions))
print 'max age:',max(age)
print 'max click',max(clicks)
print 'max impressions',max(impressions)
print 'average impressions:',float(sum(impressions)/len(impressions))
print "Summary:", data.describe()

#Draw graph
xbins=range(1,108)
plt.hist(age, bins=xbins, color='blue')
print "Age summary (ignore zero):",plt.show()

# Try to describe by age 
grouped = data.groupby('age')
grouped.describe()
