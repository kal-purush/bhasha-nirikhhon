#використані бібліотеки
from numpy import *
from matplotlib.pyplot import *
from scipy.stats import *

filename = raw_input()#бере назву файла з консолі
f = open(filename,"r")#відкриває файл
len = int(f.readline())#бере довжину з першого рядку файла
datarray = []#ініціалізує масив данних

for x in range(len):
    datarray.append(int(f.readline()))

fig = hist(datarray)#малюємо гістограму
#записуємо властивості данних в файл
title('median='+str(median(datarray))+", mode="+str(mode(datarray).mode)+", dispersion="+str(iqr(datarray))+",\nstandard deviation="+str(std(datarray))+", quartile deviation="+str((quantile(datarray,0.75)-quantile(datarray,0.25))/2))
#підписи
xlabel('value')
ylabel('frequency')
savefig("output from "+filename+".png")#зберігає файл