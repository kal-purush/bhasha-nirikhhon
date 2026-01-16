# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

"""
07102016-2.dat
"""

usinp = (raw_input('-> '))#ввод имени, открываемого файла
#print "user input: ", type(usinp)
len_usinp = len(str(usinp))
#print type(len_usinp)
usinp_e = usinp[0:-4] + 'e' + usinp[-4:len_usinp]
#print usinp_e
s = open(usinp).read()#прочитали исходный файл

#создаем датафрэйм
frame = pd.read_csv(usinp, sep = "\t", header = None, names = ['sec', 'mA', 'mV'], skiprows = 1)

allarray = np.array(frame)
#print allarray
sec = np.array(frame['sec'])
#print x
ma = np.array(frame['mA'])
#print y
mv = np.array(frame['mV'])
#print mv

#plt.figure(1)
#plt.subplot(211)
plt.plot(sec, ma)
plt.xlabel('time, sec')
plt.ylabel('I, mA')
plt.title(u'Изменение тока по времени')
plt.grid(True)


plt.figure.SubplotParams(left=1, bottom=1, right=1, top=1, wspace=1, hspace=1)
plt.figure.Figure(figsize={5:3}, dpi=300)


"""
plt.subplot(212)
plt.plot(mv, ma, '-r')
plt.xlabel('Volts, mV')
plt.ylabel('I, mA')
plt.title(u'Вольт-амперная характеристика')
plt.grid(True)
"""


plt.show()
