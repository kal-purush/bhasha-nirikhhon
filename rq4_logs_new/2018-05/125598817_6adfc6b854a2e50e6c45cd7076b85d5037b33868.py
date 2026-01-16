import pandas as pd
import numpy as np
from sklearn import linear_model
from scipy.interpolate import UnivariateSpline
import time

class Correction:

####input for bin method######

	# No. of spots need to be binned
	Spots = 30
	# No. of bin, the more the bins, the smaller the binned ATN unit
	bin_number = 30

####input for gap method######
	gap_avg_points = 3


	def bin_method(self):

		# Rawdata file, following the format in the provided template "Rawdata.xlsx"
		print '%d spots and %d bins are set to calculate filter loading parameter k.' % (self.Spots, self.bin_number)
		print ' Processing...'
		xl_file = pd.ExcelFile("rawdata.xlsx")

		################################## Main Function ##################################
		df = xl_file.parse().dropna(axis=1, how='all')
		Datetime = df.dropna().Datetime
		df.dropna()
		### identify tape advance by Null data
		null_value = df.ATN1.isnull()
		gb = df.dropna().groupby(null_value.cumsum())
		#//////////////////////////Get K value/////////////////////////////#
		R_array = []
		k_array = []
		channels = (len(df.columns) - 1) / 2
		for i in xrange(1, channels + 1):

			max_ATN = []
			for key, item in gb:
				spot = gb.get_group(key)
				max_ATN.append(spot['ATN' + str(i)].max())

			binned_group = []
			bin_time = []
			for key, item in gb:
				spot = gb.get_group(key)
				bins = np.linspace(0, np.nanpercentile(max_ATN,25), self.bin_number)
				groups = spot.groupby(np.digitize(spot['ATN' + str(i)], bins))
				binned_group.append(groups.mean())
				bin_time.append(spot['Datetime'].iloc[0])

			#print 'ATN %d Bining range is from 0 to ' % i, np.nanpercentile(max_ATN, 25)
			tape_advances = len(binned_group)

			if self.Spots >= tape_advances:
				print 'There are %d spots in this data set' % tape_advances
				print 'Input spots are greater than or equal maximum spots number, i.e., %d.\nSo there will be one filter loading parameter' % (tape_advances)
				self.bin_one_k()
				return
			binTime_df = pd.DataFrame({'TapeAdvanceTime': bin_time})

			RSq = []
			k = []
			for j in xrange(0, tape_advances - self.Spots / 2 * 2):
				df_concat = pd.concat((binned_group[j:j + self.Spots / 2 * 2]), axis=1)
				data = df_concat.groupby(level=0, axis=1).mean()

				x = data[['ATN' + str(i)]].values
				y = data['BC' + str(i)].values


				y_spl = UnivariateSpline(x, y, s=0, k=4)
				x_range = np.linspace(x[0], x[-1], len(x))

				y_spl_2d = y_spl.derivative(n=2)
				abs_y_2d = abs(y_spl_2d(x_range))

				idx = np.where(abs_y_2d > np.nanpercentile(abs_y_2d,75))
				x = np.delete(x, idx, 0)
				y = np.delete(y,idx,0)


				reg = linear_model.LinearRegression()
				reg.fit(x, y)
				R_square = reg.score(x, y)
				slope = reg.coef_[0]
				intercept = reg.intercept_
				k_value = -slope / intercept
				RSq.append(R_square)
				k.append(k_value)
			RSq = [RSq[0]] * (self.Spots / 2) + RSq + [RSq[-1]] * (self.Spots / 2)
			k = [k[0]] * (self.Spots / 2) + k + [k[-1]] * (self.Spots / 2)
			R_array = np.append(R_array, RSq, axis=0)
			k_array = np.append(k_array, k, axis=0)

		print 'There are %d spots in this data set' % tape_advances
		print "Filter loading paramter k is: "
		column_R = []
		column_k = []
		for i in xrange(1,channels+1):
			column_R.append('Rsq'+str(i))
			column_k.append('k' + str(i))
		R_array = np.reshape(R_array, (channels, tape_advances))
		k_array = np.reshape(k_array, (channels, tape_advances))
		R_df = pd.DataFrame(R_array.T)
		R_df.columns = column_R
		k_df = pd.DataFrame(k_array.T)
		k_df.columns = column_k
		print k_df
		tape_advance_data = pd.concat([binTime_df, k_df, R_df], axis=1)
		tape_advance_data.to_csv('Bin_k.csv', index=False)

		#////////////////////////////Correct Raw BC data////////////////////////////#
		BC=[]
		BC_rawdata = []
		for i in xrange(1,channels+1):
			BC_corrected = []
			BC_nc = []
			m=0
			for key, item in gb:
				spot= gb.get_group(key)
				BC_raw = spot['BC' + str(i)]
				BC_corr = spot['BC'+str(i)]/(1-spot['ATN'+str(i)]*k_array[i-1][m])
				m=m+1
				BC_corrected.append(BC_corr)
				BC_nc.append(BC_raw)
			df = pd.concat(BC_corrected).to_frame()
			df.columns = ['corrected_BC'+str(i)]
			BC.append(df)
			r_df = pd.concat(BC_nc).to_frame()
			r_df.columns = ['raw_BC' + str(i)]
			BC_rawdata.append(r_df)
		corrected_BC = pd.concat(BC,axis = 1)
		raw_BC = pd.concat(BC_rawdata,axis = 1)
		corrected_data = pd.concat((Datetime,raw_BC,corrected_BC),axis=1)
		corrected_data.to_csv('Bin_corrected_data.csv',index = False)
		print 'Correction finished, check the output CSV file'
		print 'This window will be closed in 5 mins'
		time.sleep(300)

	def bin_one_k(self):

		# Rawdata file, following the format in the provided template "Rawdata.xlsx"
		print ' Processing...'

		xl_file = pd.ExcelFile("rawdata.xlsx")

		################################## Main Function ##################################
		df = xl_file.parse().dropna(axis=1, how='all')
		Datetime = df.dropna().Datetime
		df.dropna()

		### identify tape advance by Null data
		null_value = df.ATN1.isnull()
		gb = df.dropna().groupby(null_value.cumsum())

		channels = (len(df.columns) - 1) / 2

		BC = []
		ATN = []
		RSq = []
		k = []
		for i in xrange(1, channels + 1):

			binned_group = []
			bin_time = []
			for key, item in gb:
				spot = gb.get_group(key)
				bins = np.linspace(0, df['ATN' + str(i)].max(), self.bin_number)
				groups = spot.groupby(np.digitize(spot['ATN' + str(i)], bins))
				binned_group.append(groups.mean())
				bin_time.append(spot['Datetime'].iloc[0])
			tape_advances = len(binned_group)
			binTime_df = pd.DataFrame({'TapeAdvanceTime': bin_time})

			for j in xrange(0, 1):
				df_concat = pd.concat((binned_group[j:j + tape_advances]), axis=1)
				data = df_concat.groupby(level=0, axis=1).mean()

				x = data[['ATN' + str(i)]].values
				y = data['BC' + str(i)].values

				y_spl = UnivariateSpline(x, y, s=0, k=4)
				x_range = np.linspace(x[0], x[-1], len(x))

				y_spl_2d = y_spl.derivative(n=2)
				abs_y_2d = abs(y_spl_2d(x_range))

				idx = np.where(abs_y_2d > np.nanpercentile(abs_y_2d, 90))
				x = np.delete(x, idx, 0)
				y = np.delete(y, idx, 0)

				reg = linear_model.LinearRegression()
				reg.fit(x, y)
				R_square = reg.score(x, y)
				slope = reg.coef_[0]
				intercept = reg.intercept_
				k_value = -slope / intercept

				ATN_df = data['ATN' + str(i)]
				BC_df = data['BC' + str(i)]
				ATN.append(ATN_df)
				BC.append(BC_df)
				RSq.append(R_square)
				k.append(k_value)

		BC = pd.DataFrame(BC).T
		ATN = pd.DataFrame(ATN).T
		RSq = pd.DataFrame(RSq, columns=['RSq1-x'])
		k_df = pd.DataFrame(k, columns=['k1-x'])

		tape_advance_data = pd.concat([ATN, BC], axis=1)
		k_R = pd.concat([k_df, RSq], axis=1)
		csv_input = pd.concat([tape_advance_data, k_R], axis=1)
		csv_input.to_csv('Bin_k.csv', index=False)

		print k_R

		# ////////////////////////////Correct Raw BC data////////////////////////////#
		BC = []
		BC_rawdata = []
		for i in xrange(1, channels + 1):
			BC_corrected = []
			BC_nc = []
			m = 0
			for key, item in gb:
				spot = gb.get_group(key)
				BC_raw = spot['BC' + str(i)]
				BC_corr = spot['BC' + str(i)] / (1 - spot['ATN' + str(i)] * k[i - 1])
				m = m + 1
				BC_corrected.append(BC_corr)
				BC_nc.append(BC_raw)
			df = pd.concat(BC_corrected).to_frame()
			df.columns = ['corrected_BC' + str(i)]
			BC.append(df)
			r_df = pd.concat(BC_nc).to_frame()
			r_df.columns = ['raw_BC' + str(i)]
			BC_rawdata.append(df)

		corrected_BC = pd.concat(BC, axis=1)
		raw_BC = pd.concat(BC_rawdata, axis=1)
		corrected_data = pd.concat((Datetime, raw_BC, corrected_BC), axis=1)
		corrected_data.to_csv('Bin_corrected_data.csv', index=False)
		print 'Correction finished, check the output file named "Bin_k.csv" and "Bin_corrected_data.csv"'
		print 'This window will be closed in 5 mins'
		time.sleep(300)

	def gap_method(self):

		print ' Processing...'
		xl_file = pd.ExcelFile("rawdata.xlsx")

		################################## Main Function ##################################

		# //////////////////////////data group by blank row/////////////////////////////#
		df = xl_file.parse().dropna(axis=1, how='all')
		Datetime = df.dropna().Datetime
		df.dropna()
		### identify tape advance by Null data
		null_value = df.ATN1.isnull()
		gb = df.dropna().groupby(null_value.cumsum())

		chanel = (len(df.columns) - 1) / 2
		# //////////////////////////Get K value/////////////////////////////#
		df_f = list()
		df_l = list()
		for key, item in gb:
			df_spot = gb.get_group(key)
			df_f.append(df_spot.iloc[0:self.gap_avg_points].mean(axis=0))
			df_l.append(df_spot.iloc[-self.gap_avg_points:].mean(axis=0))
		first_matrix = pd.concat(df_f, axis=1).T
		last_matrix = pd.concat(df_l, axis=1).T
		first_matrix.drop(first_matrix.head(1).index, inplace=True)
		last_matrix.drop(last_matrix.tail(1).index, inplace=True)

		first_matrix = first_matrix.reset_index(drop=True)

		df_k = (first_matrix.iloc[:, 0:chanel] - last_matrix.iloc[:, 0:chanel]) / \
			   (pd.np.multiply(last_matrix.iloc[:, 0:chanel], last_matrix.iloc[:, chanel:chanel * 2]) - pd.np.multiply(
				   first_matrix.iloc[:, 0:chanel], first_matrix.iloc[:, chanel:chanel * 2]))
		df_k.to_csv('Gap_k.csv', index=False)
		print "Filter loading paramter k is: "
		print df_k.mean()
		df_k = df_k.append(df_k.iloc[-1])
		df_k = df_k.reset_index(drop=True)

		# ////////////////////////////Correct Raw BC data////////////////////////////#
		n = 0
		BC_corr = list()
		for key, item in gb:
			spot = gb.get_group(key)
			atn = pd.DataFrame(spot.iloc[:, chanel + 1:chanel * 2 + 1].values, columns=df_k.columns)
			BC_corr.append(pd.np.multiply(spot.iloc[:, 1:chanel + 1], (1 + df_k.iloc[n] * atn)))
			n = n + 1
		BC_corr_df = pd.concat(BC_corr, axis=0)
		corrected_data = pd.concat((Datetime, BC_corr_df), axis=1)
		corrected_data.to_csv('Gap_corrected_data.csv', index=False)
		print 'Correction finished, check the output file named "Gap_k.csv" and "Gap_corrected_data.csv"'
		print 'This window will be closed in 5 mins'
		time.sleep(300)

############ Main Function##############

option = raw_input('Which method? type 1 for Bin method; 2 for Gap method: \n')
if option == '1':
	Correction.Spots = int(raw_input('Input number of spots need to be binned, default value is 30: \n'))
	Correction.bin_number = int(raw_input('Input number of bins, default value is 30: \n'))
	Correction().bin_method()
elif option == '2':
	Correction.gap_avg_points = int(raw_input('Input number of points to be averaged, default value is 3 for 1-min data resolution: \n'))
	Correction().gap_method()
else:
	print 'Wrong input, please run again and reinput'

