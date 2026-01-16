datasets_df = pd.read_csv('https://www.cs.ucr.edu/~eamonn/time_series_data_2018/DataSummary.csv')
datasets128 = datasets_df['Name'].values

#developmentes datasets
development_datasets = ['Beef', 'BirdChicken', 'Car', 'CricketX', 'CricketY', 'CricketZ', 'DistalPhalanxTW',
                        'ECG200', 'ECG5000', 'FiftyWords', 'Fish', 'FordA', 'FordB', 'Haptics', 'Herring',
                        'InsectWingbeatSound', 'ItalyPowerDemand', 'LargeKitchenAppliances', 'Lightning2',
                        'Lightning7', 'Meat', 'MedicalImages', 'MiddlePhalanxOutlineAgeGroup', 'OSULeaf',
                        'OliveOil', 'Phoneme', 'Plane', 'ProximalPhalanxOutlineCorrect', 'ProximalPhalanxTW',
                        'ScreenType', 'ShapeletSim', 'Strawberry', 'SwedishLeaf', 'SyntheticControl',
                        'ToeSegmentation1', 'Trace', 'UWaveGestureLibraryY', 'Wafer', 'WordSynonyms',
                        'Worms', 'Yoga']


varying_length_datasets = ['PLAID', 'AllGestureWiimoteX', 'AllGestureWiimoteY',
                   'AllGestureWiimoteZ', 'GestureMidAirD1', 'GestureMidAirD2',
                    'GestureMidAirD3', 'GesturePebbleZ1', 'GesturePebbleZ2',
                    'PickupGestureWiimoteZ', 'ShakeGestureWiimoteZ']



#difference between datasets with setdiff1d
validation_datasets = np.setdiff1d(datasets128, development_datasets)
validation_datasets

#remove varying length datasets from validation datasets
validation_datasets = np.setdiff1d(validation_datasets, varying_length_datasets)

#Total num of valid validation datasets and label:
print('num of valid validation datasets: ', len(validation_datasets))

#Total num of valid validation datasets and developement datasets:
print('num of valid develompent datasets: ', len(development_datasets))

#all valid datasets (datasets128 except varying length datasets)
all_valid_datasets = np.setdiff1d(datasets128, varying_length_datasets)
print('num of all valid datasets: ', len(all_valid_datasets))


#Datasetes with missing values
datasets_with_nans = ['DodgerLoopDay', 'DodgerLoopGame', 'DodgerLoopWeekend', 'MelbournePedestrian']

#all valid datasets with no Nans:
all_valid_datasets_no_nans = np.setdiff1d(all_valid_datasets,datasets_with_nans)
print('num of all valid datasets with no nans: ', len(all_valid_datasets_no_nans))