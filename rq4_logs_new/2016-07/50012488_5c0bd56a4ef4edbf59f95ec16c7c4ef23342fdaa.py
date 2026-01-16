import os
import glob
import numpy as np
import datetime
import csv

from sklearn.cross_validation import train_test_split
from sklearn.ensemble import GradientBoostingRegressor,RandomForestRegressor,ExtraTreesRegressor
from sklearn.linear_model import LinearRegression
from stacked_generalizer import StackedGeneralizer

VERBOSE = False
N_FOLDS = 5


fl = glob.glob('../data/occupancy_minutes/*more_data.csv')
print fl
f_mae = lambda x,y:(abs(x-y)).mean()
f_rms = lambda x,y:np.sqrt((np.square(x-y)).mean())
def process_one_file(f):
    print f
    csv_r = csv.reader(open(f))
    csv_r.next() # jump header
    x,y=[],[]

    for r in csv_r:
        tmp_t = datetime.datetime.strptime(r[1],'%Y-%m-%d %H:%M:%S')
        hour = r[2]
        minutes = r[3]
        v_occ_min = float(r[4])
        w_occ_min = float(r[5])
        v_occ = float(r[6])
        win = float(r[7])
        wout = float(r[8])
        raw_v_occ = float(r[9])
        _y = float(r[-1])
        x.append([hour,minutes,v_occ_min,w_occ_min,v_occ,win,wout,raw_v_occ])
        y.append(_y)

    x,y=np.array(x),np.array(y)
    x_train,x_test,y_train,y_test = train_test_split(x,y,test_size=0.3,random_state=233)

    t = GradientBoostingRegressor()
    t.fit(x_train,y_train)
    predict = t.predict(x_test)
    predict_all = t.predict(x)
    print 'gbrt',f_mae(predict,y_test),f_rms(predict,y_test),f_mae(predict_all,y),f_rms(predict_all,y)

    # define base models
    base_models = [GradientBoostingRegressor(n_estimators=100),
                   RandomForestRegressor(n_estimators=100, n_jobs=-1),
                   ExtraTreesRegressor(n_estimators=100, n_jobs=-1)]

    # define blending model
    blending_model = LinearRegression()

    # initialize multi-stage model
    sg = StackedGeneralizer(base_models, blending_model,
                            n_folds=N_FOLDS, verbose=VERBOSE)

    # fit model
    sg.fit(x_train,y_train)
    predict = sg.predict(x_test)
    predict_all = sg.predict(x)
    print 'stack', f_mae(predict, y_test), f_rms(predict, y_test), f_mae(predict_all, y), f_rms(predict_all, y)

    print ''

for f in fl:
    process_one_file(f)