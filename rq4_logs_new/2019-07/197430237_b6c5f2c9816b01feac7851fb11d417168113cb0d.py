import pandas


class dataset:
	def __init__(self, name):
		self.name = name
		self.data = pandas.read_csv(name)


	def show_dataset(self, rows):
		from pandas import set_option
		set_option('display.width', 150)	# For all collumns to be displayed side by side, otherwise, some collumns are newlined
		set_option('precision', 3)
		print ("==============  First " + str(rows) + " of "+ str(self.name) + " dataset ===================")
		print (str(self.data.head(rows)) + '\n')		# Don't need to print whole dataset


	# Functions for printing the description of 
	def stat_describe(self):
		print ("==============================  Statistical Description  ========================================")
		print(str(self.data.describe()) + '\n')
	def stat_correlate(self, method):
		print ("==============================  Statistical Correlation  ========================================")
		print(str(self.data.corr(method=method)) + '\n')
	def stat_skew(self):
		print ("=================================  Statistical Skew  ============================================")
		print(str(self.data.skew()) + '\n')


	# Functions for correlating between attributes
	def correlation_matrix(self, plot):
		figure = plot.figure()
		ax = figure.add_subplot(111)
		figure.colorbar(ax.matshow(self.data.corr(), vmin=-1, vmax=1))
	def scatter_plot(self):
		from pandas.tools.plotting import scatter_matrix as scatter
		scatter(self.data)


	def describe_data(self, stats, corr, skew):
		# stats:  dataset size, mean, std, min value, interquartile ranges, max value
		# corr:   How attributes change wrt to each other. High correlation affects regression performance
		# skew:	  ML algos assume data Gaussian distrib. Skew is how shifted distribution is, so you can normalize
		if stats == 'yes': 		self.stat_describe()
		if corr == 'yes':		self.stat_correlate('pearson')
		if skew == 'yes': 		self.stat_skew()


	def visualize_data(self, spread, correlation):
		# histogram: 	  	Discrete distrib of each attribute. Skews can be better understood
		# density plot: 	Continuous distrib of each attribute. Higher granularity than histogram
		# box and whisker:  Interquartile range of each attribute. Points beyond whiskers are candidate outliers
		# corr_matrix: 		Heatmap rendition of correlation matrix      for QUICK interpretability of correlation
		# scatter plot:	 	Attribute/attribute correlation scatter plot for DEEP interpretability of correlation
		import matplotlib.pyplot as plt
		if spread == 'histogram': 	 	self.data.hist(layout=(2,7))
		if spread == 'density plot': self.data.plot(kind='density', subplots=True, layout=(2,7), sharex=False)
		if spread == 'box and whisker': self.data.plot(kind='box',  subplots=True, layout=(2,7), sharex=False)

		if correlation == 'correlation matrix': self.correlation_matrix(plt)
		if correlation == 'scatter plot': 		self.scatter_plot()

		plt.show()