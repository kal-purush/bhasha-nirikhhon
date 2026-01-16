import matplotlib.pyplot as plt
import utilFunctionsRong as UFR
import numpy as np

titleFontSize = 25
labelFontsize = 14
legendFontsize = 12
markersize = 15

# droidTitle = fm.FontProperties(fname='font/DroidSansFallback.ttf', size = 16)
# droidLegend = fm.FontProperties(fname='font/DroidSansFallback.ttf', size = 12)
# droidTick = fm.FontProperties(fname='font/DroidSansFallback.ttf', size = 12)

# -----spectral centroid
fig, ax = plt.subplots()

color = 'r'
width = 0.35
ind1 = np.arange(5)*width
sylMeans = (2681.58, 2469.01, 2418.73, 2577.81, 2536.74)
sylStds = (406, 364, 336, 360, 367)
#xticklabels1 = ('ssqj\nLSs', 'fhc\nLYf', 'fhc\nSYh', 'fhc\nLSs','Mei\nmean')
xticklabels1 = ('Li Shengsu', 'Li Yufu', 'Shi Yihong', 'fhc\nLSs','Mei\nmean')
bar1 = plt.bar(ind1, sylMeans, width, color=color, yerr = sylStds)
# UFR.autolabelBar(bar1, ax)

color = 'y'
ind2 = np.arange(5)*width + width*6
sylMeans = (1817.72, 2022.63, 2309.16, 2396.71, 2136.55)
sylStds = (359, 460, 510, 477, 452)
xticklabels2=('ssqj\nCXq', 'sln\nCXq', 'sln\nLPh','sln\nLGj','Cheng\nmean')
bar2 = plt.bar(ind2, sylMeans, width, color=color, yerr = sylStds)
# UFR.autolabelBar(bar2, ax) # no auto label

plt.xticks(np.concatenate((ind1,ind2),0)+width/2.0, xticklabels1+xticklabels2)
plt.legend((bar1[0], bar2[0]), ('Mei', 'Cheng'), loc = 'best', prop={'size':legendFontsize})
plt.ylabel('Frequency (Hz)', fontsize = labelFontsize)
# plt.title('Spectral Centroid')
plt.show()

# -----loudness
fig, ax = plt.subplots()

color = 'r'
width = 0.35
ind1 = np.arange(5)*width
sylMeans = (0.25, 0.35, 0.27, 0.25, 0.279)
xticklabels1 = ('ssqj\nLSs', 'fhc\nLYf', 'fhc\nSYh', 'fhc\nLSs','Mei\nmean')
bar1 = plt.bar(ind1, sylMeans, width, color=color)
# UFR.autolabelBar(bar1, ax)

color = 'y'
ind2 = np.arange(5)*width + width*6
sylMeans = (0.34, 0.41, 0.46, 0.34, 0.39)
xticklabels2=('ssqj\nCXq', 'sln\nCXq', 'sln\nLPh','sln\nLGj','Cheng\nmean')
bar2 = plt.bar(ind2, sylMeans, width, color=color)
# UFR.autolabelBar(bar2, ax) # no auto label

plt.xticks(np.concatenate((ind1,ind2),0)+width/2.0, xticklabels1+xticklabels2)
plt.legend((bar1[0], bar2[0]), ('Mei', 'Cheng'), loc = 'best', prop={'size':legendFontsize})
plt.ylabel('Loudness standard deviation', fontsize = labelFontsize)
# plt.title('Loudness')
plt.show()

# -----spectral flux
color = 'r'
width = 0.35
ind1 = np.arange(5)*width
sylMeans = (0.12, 0.121, 0.123, 0.121, 0.12125)
sylStds = (0.057, 0.072, 0.065, 0.057, 0.06275)
xticklabels1 = ('ssqj\nLSs', 'fhc\nLYf', 'fhc\nSYh', 'fhc\nLSs','Mei\nmean')
#bar1 = plt.bar(ind1, sylMeans, width, color=color, yerr = sylStds)
bar1 = plt.bar(ind1, sylMeans, width, color=color)

color = 'y'
ind2 = np.arange(5)*width + width*6
sylMeans = (0.068, 0.085, 0.102, 0.091, 0.0865)
sylStds = (0.048, 0.062, 0.069, 0.053, 0.058)
xticklabels2=('ssqj\nCXq', 'sln\nCXq', 'sln\nLPh','sln\nLGj','Cheng\nmean')
# bar2 = plt.bar(ind2, sylMeans, width, color=color, yerr = sylStds)
bar2 = plt.bar(ind2, sylMeans, width, color=color)

plt.xticks(np.concatenate((ind1,ind2),0)+width/2.0, xticklabels1+xticklabels2)
plt.legend((bar1[0], bar2[0]), ('Mei', 'Cheng'), loc = 'best', prop={'size':legendFontsize})
plt.ylabel('Normed Spectral Flux', fontsize = labelFontsize)
# plt.title('Spectral Flux')
plt.show()

# ----- tristimulus comp. 1 and 2
fig, ax = plt.subplots()
styles = ('ro','y^')
trist0x = (0.22, 0.143, 0.236, 0.231, 0.207)
trist0y = (0.642, 0.735, 0.616, 0.591, 0.646)
xticklabels1 = ('fhc\nLYf', 'fhc\nSYh', 'fhc\nLSs','ssqj\nLSs','Mei\nmean')
tristPlt0 = plt.plot(trist0x, trist0y, styles[0], markersize = markersize)

trist1x = (0.344, 0.331, 0.27, 0.257, 0.301)
trist1y = (0.594, 0.584, 0.591, 0.601, 0.593)
xticklabels2=('ssqj\nCXq', 'sln\nCXq', 'sln\nLPh','sln\nLGj','Cheng\nmean')
tristPlt1 = plt.plot(trist1x, trist1y, styles[1], markersize = markersize)

plt.legend((tristPlt0[0], tristPlt1[0]), ('Mei', 'Cheng'), loc = 'best')
ax.set_xlabel('Trist. comp. 1', fontsize = labelFontsize)
ax.set_ylabel('Trist. comp. 2', fontsize = labelFontsize)
# ax.set_title('Tristimulus', fontsize = titleFontSize)
plt.xlim((0.12, 0.37))
plt.ylim(0.56, 0.77)
# plt.xlim((0, 1))
# plt.ylim((0, 1))

tristx = trist0x+trist1x
tristy = trist0y+trist1y
text = xticklabels1 + xticklabels2

ax.annotate(text[3], (tristx[3],tristy[3]), xytext=(-30, 0), textcoords='offset points')
ax.annotate(text[4], (tristx[4],tristy[4]), xytext=(-40, 0), textcoords='offset points')

for i in range(len(tristx)):
    if i != 3 and i != 4:
        ax.annotate(text[i], (tristx[i],tristy[i]), xytext=(-10, 10), textcoords='offset points')
plt.show()

# ----- tristimulus comp. 2 and 3
fig, ax = plt.subplots()

styles = ('ro','y^')
trist0x = (0.642, 0.735, 0.616, 0.591, 0.646)
trist0y = (0.138, 0.121, 0.149, 0.177, 0.146)
xticklabels1 = ('fhc\nLYf', 'fhc\nSYh', 'fhc\nLSs','ssqj\nLSs','Mei\nmean')
tristPlt0 = plt.plot(trist0x, trist0y, styles[0], markersize = markersize)

trist1x = (0.594, 0.584, 0.591, 0.601, 0.593)
trist1y = (0.062, 0.085, 0.139, 0.142, 0.107)
xticklabels2=('ssqj\nCXq', 'sln\nCXq', 'sln\nLPh','sln\nLGj','Cheng\nmean')
tristPlt1 = plt.plot(trist1x, trist1y, styles[1], markersize = markersize)

plt.legend((tristPlt0[0], tristPlt1[0]), ('Mei', 'Cheng'), loc = 'best', prop={'size':legendFontsize})
plt.xlabel('Trist. comp. 2', fontsize = labelFontsize)
plt.ylabel('Trist. comp. 3', fontsize = labelFontsize)
# ax.set_title('Tristimulus', fontsize = titleFontSize)
plt.xlim((0.56, 0.76))
plt.ylim(0.04, 0.20)
# plt.xlim((0, 1))
# plt.ylim((0, 1))

tristx = trist0x+trist1x
tristy = trist0y+trist1y
text = xticklabels1 + xticklabels2

ax.annotate(text[0], (tristx[0],tristy[0]), xytext=(-30, 0), textcoords='offset points')
ax.annotate(text[5], (tristx[5],tristy[5]), xytext=(-5, 10), textcoords='offset points')
ax.annotate(text[7], (tristx[7],tristy[7]), xytext=(-30, 0), textcoords='offset points')

for i in range(len(tristx)):
    if i != 0 and i != 5 and i != 7:
        ax.annotate(text[i], (tristx[i],tristy[i]), xytext=(-10, 10), textcoords='offset points')
plt.show()


