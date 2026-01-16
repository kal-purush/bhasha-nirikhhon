#Power Efficient Dead-Reckoning of Animal Movement Using IMU Data
#03/22/2017
#UCLA EE202B - Cyber Physical Systems at UCLA
#Advised by Professor Mani Srivastava
#Jeya Vikranth Jeyakumar: vikranth.1994@gmail.com
#Vivi Tzu-Wei Chuang: vivi51123@gmail.com
##############################################################################
# Processed data collected by Vikranth's mobile phone using MotionTracker App
# GPS latitude and longitude and IMU readings
# 20 minute walk 
##############################################################################

import sklearn
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")
warnings.filterwarnings(action="ignore", category=DeprecationWarning)

from sklearn import linear_model
from sklearn.linear_model import LinearRegression, LogisticRegression

from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline

from sklearn.ensemble import RandomForestRegressor

from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

import statsmodels.api as sm

from scipy import interpolate
import pandas as pd
import collections
import numpy as np
import math

##############################################################################
# 18 features + Train first last then predict middle + sample before
def p_1():

	m = pd.read_csv('./matrix1.csv',header=0, names=['t','acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long'])
	m2 = pd.read_csv('./matrix2.csv',header=0, names=['t','acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long'])

	train_length = [1, 0.7, 0.5]
	title = "Train " + str(1+2) + " (" + str(2060) + "%) Test " + str(1+2) + " (" + str(20) + "%) - 7" 
	print title
	print ("\n")

	# Adjust sampling rate
	m = m[0:len(m):1]
	# m = m[0:len(m):5]
	# m = m[0:len(m):10]

	#m
	# Sample rate 1
	index = 0
	for i in range(0, len(m.lat)-1):
		dist_x, dist_y = getDistance(m.lat[i], m.lat[i+1], m.long[i], m.long[i+1])
		m.set_value(index, 'lat', dist_x)
		m.set_value(index, 'long', dist_y)
		index += 1

#    # Sample rate 5
#	index = 0
#	for i in range(0, 5*len(m.lat)-5, 5):
#		dist_x, dist_y = getDistance(m.lat[i], m.lat[i+5], m.long[i], m.long[i+5])
#		m.set_value(index, 'lat', dist_x)
#		m.set_value(index, 'long', dist_y)
#		index += 5
#    
#    # Sample rate 10
#     index = 0
#     for i in range(0, 10*len(m.lat)-10, 10):
#     	dist_x, dist_y = getDistance(m.lat[i], m.lat[i+10], m.long[i], m.long[i+10])
#     	m.set_value(index, 'lat', dist_x)
#     	m.set_value(index, 'long', dist_y)
#     	index += 10

	# Append previous data as features for current one
	m_add = m
	m_add['acc_x_2'] = [0]* len(m_add)
	m_add['acc_x_2'][0:-1] = m_add['acc_x'][1:]

	m_add['acc_y_2'] = [0]* len(m_add)
	m_add['acc_y_2'][0:-1] = m_add['acc_y'][1:]

	m_add['acc_z_2'] = [0]* len(m_add)
	m_add['acc_z_2'][0:-1] = m_add['acc_z'][1:]

	m_add['rot_x_2'] = [0]* len(m_add)
	m_add['rot_x_2'][0:-1] = m_add['rot_x'][1:]

	m_add['rot_y_2'] = [0]* len(m_add)
	m_add['rot_y_2'][0:-1] = m_add['rot_y'][1:]

	m_add['rot_z_2'] = [0]* len(m_add)
	m_add['rot_z_2'][0:-1] = m_add['rot_z'][1:]

	m_add['mag_x_2'] = [0]* len(m_add)
	m_add['mag_x_2'][0:-1] = m_add['mag_x'][1:]

	m_add['mag_y_2'] = [0]* len(m_add)
	m_add['mag_y_2'][0:-1] = m_add['mag_y'][1:]

	m_add['mag_z_2'] = [0]* len(m_add)
	m_add['mag_z_2'][0:-1] = m_add['mag_z'][1:]

	#print m_add
	m = m_add
	# Remove last row cuz no diff
	m = m[:-1]
	
	##################################################################
	#m2
	m2 = m2[0:len(m2):1]
	# m2 = m2[0:len(m2):5]
	# m2 = m2[0:len(m2):10]
	index = 0
	for i in range(0, len(m2.lat)-1):
		dist_x, dist_y = getDistance(m2.lat[i], m2.lat[i+1], m2.long[i], m2.long[i+1])
		m2.set_value(index, 'lat', dist_x)
		m2.set_value(index, 'long', dist_y)
		index += 1
    
#    # Sample rate 5
#	index = 0
#	for i in range(0, 5*len(m2.lat)-5, 5):
#		dist_x, dist_y = getDistance(m2.lat[i], m2.lat[i+5], m2.long[i], m2.long[i+5])
#		m2.set_value(index, 'lat', dist_x)
#		m2.set_value(index, 'long', dist_y)
#		index += 5
#
#    # Sample rate 10
#    index = 0
#    for i in range(0, 10*len(m2.lat)-10, 10):
#     	dist_x, dist_y = getDistance(m2.lat[i], m2.lat[i+10], m2.long[i], m2.long[i+10])
#     	m2.set_value(index, 'lat', dist_x)
#     	m2.set_value(index, 'long', dist_y)
#     	index += 10

	m_add_2 = m2
	m_add_2['acc_x_2'] = [0]* len(m_add_2)
	m_add_2['acc_x_2'][0:-1] = m_add_2['acc_x'][1:]

	m_add_2['acc_y_2'] = [0]* len(m_add_2)
	m_add_2['acc_y_2'][0:-1] = m_add_2['acc_y'][1:]

	m_add_2['acc_z_2'] = [0]* len(m_add_2)
	m_add_2['acc_z_2'][0:-1] = m_add_2['acc_z'][1:]

	m_add_2['rot_x_2'] = [0]* len(m_add_2)
	m_add_2['rot_x_2'][0:-1] = m_add_2['rot_x'][1:]

	m_add_2['rot_y_2'] = [0]* len(m_add_2)
	m_add_2['rot_y_2'][0:-1] = m_add_2['rot_y'][1:]

	m_add_2['rot_z_2'] = [0]* len(m_add_2)
	m_add_2['rot_z_2'][0:-1] = m_add_2['rot_z'][1:]

	m_add_2['mag_x_2'] = [0]* len(m_add_2)
	m_add_2['mag_x_2'][0:-1] = m_add_2['mag_x'][1:]

	m_add_2['mag_y_2'] = [0]* len(m_add_2)
	m_add_2['mag_y_2'][0:-1] = m_add_2['mag_y'][1:]

	m_add_2['mag_z_2'] = [0]* len(m_add_2)
	m_add_2['mag_z_2'][0:-1] = m_add_2['mag_z'][1:]

	m2 = m_add_2
	# Remove last row cuz no diff
	m2 = m2[:-1]

	##################################################################
	m = pd.concat([m, m2])
	##################################################################

	# Get proper features and target data from m
	#train_length = int(round(0.8*len(m.index)))
	m_first = pd.DataFrame(m[0:int(round(0.2*len(m.index)))], columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])
	m_last = pd.DataFrame(m[int(round(0.4*len(m.index))):], columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])
	
	m_f_l = pd.concat([m_first, m_last])

	m_80 = pd.DataFrame(m_f_l, columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])
	
	m_middle = pd.DataFrame(m[int(round(0.2*len(m.index))):int(round(0.4*len(m.index)))], columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])
	m_20 = pd.DataFrame(m_middle, columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','lat','long','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])

	# Ground truth entire path
	m_lat = np.cumsum(m.lat.tolist())
	m_lon = np.cumsum(m.long.tolist())

	offset_x = np.sum(m_80.lat[1:len(m_first)])
	offset_y = np.sum(m_80.long[1:len(m_first)])

	##################################################################

	features = m_80.as_matrix(columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])
	target = m_80.as_matrix(columns=['lat', 'long'])

	features_test = m_20.as_matrix(columns=['acc_x','acc_y','acc_z','rot_x','rot_y','rot_z','mag_x','mag_y','mag_z','acc_x_2','acc_y_2','acc_z_2','rot_x_2','rot_y_2','rot_z_2','mag_x_2','mag_y_2','mag_z_2'])
	target_test = m_20.as_matrix(columns=['lat', 'long'])

	##################################################################
	
	# Linear Regression and the root mean squared error
	print "(Linear Regression)"
	reg = linear_model.LinearRegression()
	reg = reg.fit(features, target[:,0])
	coef_lat = reg.coef_
	res_lat = reg.residues_
	intercept_lat = reg.intercept_
	lat = reg.predict(features_test)

	reg = linear_model.LinearRegression()
	reg = reg.fit(features, target[:,1])
	coef_long = reg.coef_
	res_long = reg.residues_
	intercept_long = reg.intercept_
	lon = reg.predict(features_test)

	# For plotting purpose, do cumsum
	lat_pred_l = np.cumsum(lat)
	lon_pred_l = np.cumsum(lon)

	# Attributes
	print 'Beta_lat:', coef_lat
	print 'Beta_long:', coef_long

	print 'Residues_lat:', res_lat   				#sum of residuals
	print 'Residues_long:', res_long

	# Calculate statistics
	target_test_0 = target_test[:,0]
	target_test_1 = target_test[:,1]
	print ("RMSE_lat: %.5f" % (mean_squared_error(target_test_0, lat) ** 0.5))
	print ("RMSE_lon: %.5f" % (mean_squared_error(target_test_1, lon) ** 0.5))

	print ("R^2_lat: %.5f" % (r2_score(target_test_0, lat)))
	print ("R^2_lon: %.5f" % (r2_score(target_test_1, lon)))

	print ("\n")
	##################################################################
	# Linear Regression and the root mean squared error
	print "(Linear Regression - statsmodels)"
	#print sklearn.feature_selection.f_regression(features, target[:,0], center=True)
	model_ols = sm.OLS(target[:,0],features)
	ols_results = model_ols.fit()
	print(ols_results.summary())

	print ("\n")

	model_ols = sm.OLS(target[:,1],features)
	ols_results = model_ols.fit()
	print(ols_results.summary())

	print ("\n")
	##################################################################
	print "(Quadratic Regression)"
	reg = Pipeline([('poly', PolynomialFeatures(degree = 2)), ('linear', LinearRegression(fit_intercept = False))])
	reg = reg.fit(features, target[:,0])
	lat = reg.predict(features_test)

	reg = Pipeline([('poly', PolynomialFeatures(degree = 2)), ('linear', LinearRegression(fit_intercept = False))])
	reg = reg.fit(features, target[:,1])
	lon = reg.predict(features_test)

	# For plotting purpose, do cumsum
	lat_pred_p = np.cumsum(lat)
	lon_pred_p = np.cumsum(lon)

	# Calculate statistics
	print ("RMSE_lat: %.5f" % (mean_squared_error(target_test_0, lat) ** 0.5))
	print ("RMSE_lon: %.5f" % (mean_squared_error(target_test_1, lon) ** 0.5))

	print ("R^2_lat: %.5f" % (r2_score(target_test_0, lat)))
	print ("R^2_lon: %.5f" % (r2_score(target_test_1, lon)))

	print ("\n")

	##################################################################
	labels = ['Train', 'Linear', 'Poly']

	# Ground truth entire path
	plt.plot(m_lat, m_lon, label=labels[0], color='k')

	# Testing set
	plt.plot(lat_pred_l+offset_x, lon_pred_l+offset_y, label=labels[1], color='m')
	plt.plot(lat_pred_p+offset_x, lon_pred_p+offset_y, label=labels[2], color='g')
	plt.title(title)
	plt.ylabel('Longitude -> distance (m)')
	plt.xlabel('Latitude -> distance (m)')
	plt.legend(loc=2) #upper left =2 lower left=3, lower right 4
	plt.show()	

######################################################################
def getDistance(Lat1, Lat2, Lon1, Lon2): 

	latMid = (Lat1+Lat2 )/2.0  # or just use Lat1 for slightly less accurate estimate
	m_per_deg_lat = 111132.954 - 559.822 * math.cos( 2.0 * latMid ) + 1.175 * math.cos( 4.0 * latMid)
	m_per_deg_lon = 111132.954 * math.cos ( latMid )

	deltaLat = (Lat2 - Lat1)
	deltaLon = (Lon2 - Lon1)

	dist_x = deltaLat * m_per_deg_lat 
	dist_y = deltaLon * m_per_deg_lon

	return dist_x, dist_y

##################################################################
def main(): 
    p_1()

if __name__ == "__main__": main()