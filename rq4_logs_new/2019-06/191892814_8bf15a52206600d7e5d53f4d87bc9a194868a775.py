from scipy.stats import lognorm, norm, triang
from ..constants import *
from itertools import compress



def get_distr_indices_params(params,id_distr):
	list_ = params['uncertainty_type']==id_distr
	indices = list(compress(range(len(list_)), list_))
	params = params[indices]
	indices = np.array(indices,dtype=int)
	return indices,params



def convert_sample_to_lognor(params,sample):
	"""
	Convert uniform in [0,1] to LOGNORMAL distribution
	"""
	q = (Q_HIGH-Q_LOW)*sample + Q_LOW
	params_converted = lognorm.ppf(q,s=params['scale'],scale=np.exp(params['loc']))
	return params_converted



def convert_sample_to_normal(params,sample):
	"""
	Convert uniform in [0,1] to NORMAL distribution
	"""
	q = (Q_HIGH-Q_LOW)*sample + Q_LOW
	params_converted  = norm.ppf(q,loc=params['loc'],scale=params['scale'])
	return params_converted



def convert_sample_to_triang(params,sample):
	"""
	Convert uniform in [0,1] to TRIANGULAR distribution
	"""
	loc   =  params['minimum']
	scale =  params['maximum'] - params['minimum']
	c     = (params['loc'] - loc) / scale
	params_converted = triang.ppf(sample,c=c,loc=loc,scale=scale)
	return params_converted



def convert_sample_to_unifor(params,sample):
	"""
	Convert uniform in [0,1] to UNIFORM distribution
	"""
	params_converted = sample * (params["maximum"]-params["minimum"]) +  params["minimum"]
	return params_converted



def convert_sample_to_dscr_u(params,sample):
	"""
	Convert uniform in [0,1] to DISCRETE UNIFORM distribution
	"""
	#-1 because stats array excludes the upper bound
	params_converted = np.rint( sample * (params["maximum"]-1 - params["minimum"]) +  params["minimum"] )
	return params_converted