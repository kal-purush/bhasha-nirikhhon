from matplotlib import pyplot as plt
import numpy as np

slices = [1433783686,1366417754,329064917,270625568,216565318]
lables = ['china','india','US','Indonasia','pakistan']

explode = [0,0,0.2,0,0]
colors = ['#C70039','#FF5733','#FFC300','#DAF7A6']

plt.pie(slices,labels=lables,explode=explode,shadow=True,startangle=90,wedgeprops={'edgecolor':'black','linewidth':1},colors=colors)
plt.title("population pie chart")
plt.grid(True)
plt.show()