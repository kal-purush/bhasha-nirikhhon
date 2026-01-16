import pandas
import datetime
import numpy as np
import math
import matplotlib.pyplot as plt

# open the CSV
df = pandas.read_csv("dfw_final.csv")
dates = df['dates']
highs = df['highs']
lows = df['lows']

# convert dates to datetimes
datetimes = [datetime.datetime.strptime(x,'%m/%d/%Y') for x in dates]

# create the lists
jan = []
feb = []
mar = []
apr = []
may = []
jun = []
jul = []
aug = []
sep = []
octo = []
nov = []
dec = []

# get daily average temperatures
for i in range(len(datetimes)):
    if datetimes[i].month == 1:
        jan.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 2:
        feb.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 3:
        mar.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 4:
        apr.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 5:
        may.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 6:
        jun.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 7:
        jul.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 8:
        aug.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 9:
        sep.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 10:
        octo.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 11:
        nov.append((highs[i] + lows[i]) / 2.0)
    elif datetimes[i].month == 12:
        dec.append((highs[i] + lows[i]) / 2.0)
    else:
        raise Exception("Invalid month!")

list_of_lists = [jan,feb,mar,apr,may,jun,jul,aug,sep,octo,nov,dec]

# compute monthly average temperatures
monthly_avg = []
monthly_std = []
monthly_lower = []
monthly_upper = []
for i in range(len(list_of_lists)):
    monthly_avg.append(np.mean(list_of_lists[i]))
    monthly_std.append(np.std(list_of_lists[i]))
    monthly_upper.append(np.percentile(list_of_lists[i],90.0))
    monthly_lower.append(np.percentile(list_of_lists[i],10.0))

# plot the results
fig = plt.figure(figsize=(16,12),dpi=80,edgecolor="k")
ax = fig.add_subplot(1,1,1)
major_ticks = np.arange(30,101,5)
minor_ticks = np.arange(30,101,5)
ax.set_xticks(np.arange(len(list_of_lists)))
ax.set_yticks(major_ticks)
ax.set_yticks(minor_ticks,minor=True)
ax.grid(which="both")
ax.grid(which="major",alpha=1.0)
ax.grid(which="minor",alpha=0.2)
dummies = np.arange(len(list_of_lists))
plt.plot(dummies,monthly_upper,color="red",linestyle="-",linewidth=4.0,label="90th Percentile")
plt.plot(dummies,monthly_lower,color="blue",linestyle="-",linewidth=4.0,label="10th Percentile")
plt.plot(dummies,monthly_avg,color="black",linestyle="-",linewidth=4.0,label="Mean")
# plot aesthetics
plt.xticks(dummies,['J','F','M','A','M','J','J','A','S','O','N','D'],rotation=90)
plt.xlim([0,11])
plt.ylim([30,100])
plt.xlabel("Month")
plt.ylabel("Temperature (degrees Fahrenheit)")
plt.title("Monthly Average Temperature")
plt.legend(bbox_to_anchor=(1,1),loc="upper left",ncol=1,fontsize="x-small")
# save the figure
plt.savefig("monthly.png",bbox_inches="tight")
plt.clf()