import csv
import scipy
import random
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import normalize
import numpy as np
def fas(x):
    """
    Formats an integer with commas as thousand separators.
    """
    return f"{x:,}"
with open('bruh1.csv', 'r') as f:
    reader = csv.reader(f)
    stuff= []
    rangey = {}
    #start is 64 for median
    #start is 58 for single
    for row in reader:
        try:
            int(row[0])
            #12
            for i in range(4,10):
                try:
                    print(row[54+i])
                    rangey[(int(row[60+i])//1000-30)//12.75] += int(row[54+i])
                except:
                    rangey[(int(row[60+i])//1000-30)//12.75] = int(row[54+i])
        except:
            pass
    # explode = [random.randrange(0,3)/10 for i in range(4)]
    fig = plt.figure(figsize =(10, 7))
    plt.pie([i for i in rangey.values()], labels = [str(fas(int(((i*12.75)+30)*1000))) + "-" + str(fas(int((((i+1)*12.75)+30)*1000))) for i in rangey.keys()], shadow=True)
    print(rangey)
    # show plot
    plt.show()