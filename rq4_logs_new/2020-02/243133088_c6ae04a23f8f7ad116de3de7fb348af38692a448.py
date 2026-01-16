import numpy as np


class training_event:
    
    def __init__(self,a,b,c):
        self.date=a
        self.effort=b
        self.duration=c



class forecast:

    def __init__(self,in_array):
        self.a=in_array
        return


    def forecast(self,h):
        horizon=h
        return np.concatenate((self.a,np.zeros(h)))


class athlete:

    def __init__(self):
        self.training_history=set([])
        return


a=np.array([1,2,3,4,3])
my_forecast=forecast(a)
print my_forecast.forecast(3)
my_athlete=athlete()
