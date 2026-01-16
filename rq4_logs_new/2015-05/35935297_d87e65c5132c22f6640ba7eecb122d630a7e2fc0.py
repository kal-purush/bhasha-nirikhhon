import sys;
import numpy as np;
from datetime import datetime, date;
import model
import warnings;
from sklearn.neighbors import KNeighborsRegressor;
import numpy  as np;
from sklearn.preprocessing import Imputer;
from sklearn.grid_search import ParameterGrid;
from multiprocessing import Pool;
import copy;
import random;
import sys;
import warnings;
from sklearn.cross_validation import train_test_split
from sklearn.metrics import confusion_matrix;
import matplotlib.pyplot as plt;
import numpy as np
import scipy.sparse
import pickle
import xgboost as xgb
import copy
from sklearn.preprocessing import OneHotEncoder
from random import randint
from random import shuffle
import math
import copy
from sklearn.neighbors import KNeighborsRegressor
from sklearn.metrics import mean_squared_error

def get_season(dateObj):
	#Order - Winter, Spring,Summer, Autumn, Winter
	seasons = [(0, (date(1,  1,  1),  date(1,  3, 20))),\
				(1, (date(1,  3, 21),  date(1,  6, 20))),\
				(2, (date(1,  6, 21),  date(1,  9, 22))),\
				(3, (date(1,  9, 23),  date(1, 12, 20))),\
				(0, (date(1, 12, 21),  date(1, 12, 31)))];

	try : 
		dateObj = dateObj.date().replace(year=1);
		for season, (start, end) in seasons:
			if start <= dateObj <= end:
				return season;
	except :
		return 0;


def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def getEventType(codesums, precipitation, snowfall):
	if ("SN" in codesums or "SG" in codesums) and isfloat(snowfall) and float(snowfall) > 2.0:
	   return 1; #Snow Event
	elif ("RA" in codesums or "SN" not in codesums) and isfloat(precipitation) and float(precipitation) > 1.0:
	   return 2; #Rain Event
	else :
	   return 0; #No Event



def read(key_file, train_file, test_file, weather_file):
	store_station_map 	   		  = dict();
	station_store_map 	   		  = dict();
	train_data            		  = [];
	test_data			  		  = [];
	station_dt_weatherDetails_map = dict();
	store_product_counts  	      = dict();

	with open(key_file, "r") as f:
		header = f.readline();

		for line in f:
			 data = line.strip().split(',');
			 store_station_map[int(data[0])] = data[1];

			 if data[1] not in station_store_map:
			 	station_store_map[data[1]] 	 = [int(data[0])];
			 else:
			 	station_store_map[data[1]].append(int(data[0]));


	with open(train_file, "r") as f:
		header = f.readline();

		for line in f:
			 data = line.strip().split(',');
			 train_data.append([data[0], int(data[1]), int(data[2]), float(data[3])]);
			 store = int(data[1]);
			 item  = int(data[2]);
			 if (store, item) in store_product_counts:
			 	store_product_counts[(store, item)] += float(data[3])
			 else:
			 	store_product_counts[(store, item)]  = float(data[3])	
			 

	with open(test_file, "r") as f:
		header = f.readline();

		for line in f:
			 data = line.strip().split(',');
			 test_data.append([data[0], int(data[1]), int(data[2])]);

	with open(weather_file, "r") as f:
		header = f.readline();

		for line in f:
			 data = line.strip().split(',');
			 station_dt_weatherDetails_map[(data[0], data[1])] = [x.replace('"','')  for x in data[2:]] 


	return store_station_map, station_store_map, train_data, test_data, station_dt_weatherDetails_map, store_product_counts;



def generateFeatures(store_station_map, station_store_map, data, station_dt_weatherDetails_map, train, store_product_counts):


	if train:
		store_item_features = dict()
	else:
		store_item_features = [];

	#Train
	for td in data:
		X = [];

		store = td[1];
		item  = td[2];

		if train:
		    if (store, item) in store_product_counts and store_product_counts[(store, item)] > 0:
				#Make date object
				dt = datetime.strptime(td[0],'%Y-%m-%d')
				#Extract date based features
				X.append(dt.month);             	 #Month
				X.append(dt.strftime("%W"));	 	 #WeekNo of the year
				X.append(((dt.day - 1) // 7 + 1));	 #Week of the month
				X.append(dt.weekday());				 #Week day 
				X.append(get_season(dt)); 			 #Season 	
				if dt.weekday() in [5,6]:
					X.append(1);			 		 #Week end
				else:
					X.append(0);

				if (store, item) in store_item_features:
					store_item_features[(store, item)][0].append(X)
					store_item_features[(store, item)][1].append( math.log(1 + td[3]) );
				else:
					store_item_features[(store, item)] = [ [X], [ math.log(1 + td[3]) ] ]
					
		else:
			#Make date object
			dt = datetime.strptime(td[0],'%Y-%m-%d')
			#Extract date based features
			X.append(dt.month);             	 #Month
			X.append(dt.strftime("%W"));	 	 #WeekNo of the year
			X.append(((dt.day - 1) // 7 + 1));	 #Week of the month
			X.append(dt.weekday());				 #Week day 
			X.append(get_season(dt)); 			 #Season 	
			if dt.weekday() in [5,6]:
				X.append(1);			 		 #Week end
			else:
				X.append(0);

			store_item_features.append( (store, item, td[0], X)   )	
				

	#####################################				
	return store_item_features;

def generateParams():
    # Set the parameters by cross-validation
    paramaters_grid    = {'n_neighbors': [7,8,9,10,12,14,15,20,25,50,100,150], 'algorithm' : ['auto'], 'weights' : ['distance', 'uniform']};

    paramaters_search  = list(ParameterGrid(paramaters_grid));

    parameters_to_try  = [];
    for ps in paramaters_search:
        params           = {};
        for param in ps.keys():
            params[str(param)] = ps[param];
        parameters_to_try.append(copy.copy(params));

    return parameters_to_try;     


if __name__ == '__main__':
	warnings.filterwarnings("ignore");
	store_station_map, station_store_map, train_data, test_data, station_dt_weatherDetails_map, store_product_counts = read("./data/key.csv", "./data/train.csv", "./data/test.csv", "./data/weather.csv");	
	print("Files Read")
	train_store_item_features = generateFeatures(store_station_map, station_store_map, train_data, station_dt_weatherDetails_map, True, store_product_counts);
	print("Training Data Loaded")
	test_store_item_features  = generateFeatures(store_station_map, station_store_map, test_data, station_dt_weatherDetails_map, False, store_product_counts);
	print("Test Data Loaded")
	del store_station_map, station_store_map, train_data, test_data, station_dt_weatherDetails_map;

	models = dict()

	for store_item in train_store_item_features:
		data = train_store_item_features[store_item];
		X_train, X_validation, Y_train, Y_validation = train_test_split(data[0], data[1], test_size=0.10, random_state=100);
		parameters_to_try = generateParams();

		best_score = sys.float_info.max;
		best_model = None;


		for i in range(0, len(parameters_to_try)):
			param     = parameters_to_try[i];
			neigh 	  = KNeighborsRegressor(**param);
			neigh.fit(X_train, Y_train);
			Y_hat = neigh.predict(X_validation)
			try:
				current_score = mean_squared_error(Y_validation, Y_hat)
			except:
				current_score = sys.float_info.max;

			if current_score < best_score:
				best_score = current_score
				best_model = neigh

		print("Best validation score for " + str(store_item) + "  is " + str(best_score));	
		models[store_item] = best_model;


		
	f = open("./data/submission.csv", "w");
	f.write("id,units\n");

	for i in range(0, len(test_store_item_features)):
	   dP = test_store_item_features[i]
	   store_item = (dP[0], dP[1])

	   if store_item not in models:
	   	  #Unseens
	   	  f.write( str(dP[0]) + "_" + str(dP[1]) + "_" + str(dP[2]) + "," + str(0) + "\n");
	   elif store_product_counts[store_item] == 0:
	   	  f.write( str(dP[0]) + "_" + str(dP[1]) + "_" + str(dP[2]) + "," + str(0) + "\n");	
	   else: 
	      f.write( str(dP[0]) + "_" + str(dP[1]) + "_" + str(dP[2]) + "," + str(models[store_item].predict(dP[3])[0]) + "\n");		


	f.close();
	


	




