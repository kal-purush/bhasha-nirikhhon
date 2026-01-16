import theano
import theano.tensor as T
import numpy as np

# LSTM network
class LSTM(object):
		
	def __init__(self, n_in, n_hidden, n_out):
		dtype = theano.config.floatX
		n_i = n_f = n_c = n_o = n_hidden

		#Init weights	
		def init_weights(start, end):
			values = np.random.uniform(low=-0.1, high=0.1, size=(start, end))
			return values
		
		W_xi = theano.shared(init_weights(n_in, n_i))
		W_hi = theano.shared(init_weights(n_hidden, n_i))
		W_ci = theano.shared(init_weights(n_c, n_i))
		b_i = theano.shared(np.random.uniform(low=-0.5, high=0.5, size=(n_i)))
		W_xf = theano.shared(init_weights(n_in, n_f))
		W_hf = theano.shared(init_weights(n_hidden, n_f))
		W_cf = theano.shared(init_weights(n_c, n_f))
		b_f = theano.shared(np.random.uniform(low=0, high=1., size=(n_f)))
		W_xc = theano.shared(init_weights(n_in, n_c))
		W_hc = theano.shared(init_weights(n_hidden, n_c))
		b_c = theano.shared(np.zeros((n_c,), dtype=dtype))
		W_xo = theano.shared(init_weights(n_in, n_o))
		W_ho = theano.shared(init_weights(n_hidden, n_o))
		W_co = theano.shared(init_weights(n_c, n_o))
		b_o = theano.shared(np.random.uniform(low=-0.5, high=0.5, size=(n_o)))
		W_hy = theano.shared(init_weights(n_hidden, n_out))
		b_y = theano.shared(np.zeros(n_out, dtype=dtype))
		
		c0 = theano.shared(np.zeros((n_hidden,), dtype=dtype))
		h0 = T.tanh(c0)
		
		#Params
		params = [W_xi, W_hi, W_ci, W_xf, W_hf, W_cf, 
				  W_xc, W_hc, W_xo, W_ho, W_co, W_hy,
				  b_i, b_f, b_c, b_o, b_y, c0]
		self.params = params
				
		#Tensor variables
		x = T.matrix()
		t = T.matrix()
		lr = T.scalar()
		
		#LSTM
		[h, c, y], _ = theano.scan(fn = self.recurrent_fn, sequences = x,
                             outputs_info  = [h0, c0, None], #corresponds to return type of fn
                             non_sequences = [W_xi, W_hi, W_ci, b_i, W_xf, W_hf, W_cf, b_f, 
                                              W_xc, W_hc, b_c, W_xo, W_ho, b_o, W_co, W_hy, b_y])
            	
		#Cost
		cost = (T.sqrt((t - y)**2)).mean()
		
		#Updates
		updates = self.RMSprop(cost, params, learnrate=lr)

		#Theano Functions
		self.train = theano.function([x, t, lr], cost, 
                                     on_unused_input='warn', 
                                     updates=updates)
		self.validate = theano.function([x, t], cost)							 
		self.predict = theano.function([x], y)	
		
    #LSTM step
	def recurrent_fn(self, x_t, h_tm1, c_tm1, 
                     W_xi, W_hi, W_ci, b_i, W_xf, W_hf, W_cf, b_f, 
	                 W_xc, W_hc, b_c, W_xo, W_ho, b_o, W_co, W_hy, b_y):
		#Input Gate
		i_t = T.nnet.sigmoid(T.dot(x_t, W_xi) + T.dot(h_tm1, W_hi) + T.dot(c_tm1, W_ci) + b_i)         		
		#Forget Gate
		f_t = T.nnet.sigmoid(T.dot(x_t, W_xf) + T.dot(h_tm1, W_hf) + T.dot(c_tm1, W_cf) + b_f)		
		#Cell
		c_t = f_t * c_tm1 + i_t * T.tanh(T.dot(x_t, W_xc) + T.dot(h_tm1, W_hc) + b_c)
		#Output Gate
		o_t = T.nnet.sigmoid(T.dot(x_t, W_xo) + T.dot(h_tm1, W_ho) + T.dot(c_t, W_co) + b_o)
		#Hidden to Hidden
		h_t = o_t * T.tanh(c_t)		
		#Output
		y_t = T.dot(h_t, W_hy) + b_y
		return [h_t, c_t, y_t]
	
	#RMSprop
	def RMSprop(self, cost, params, learnrate=0.01, rho=0.9, epsilon=1e-6):
		gparams = []
		for param in params:
			gparam = T.grad(cost, param)
			gparams.append(gparam)	
		updates=[]
		for param, gparam in zip(params, gparams):
			acc = theano.shared(param.get_value() * 0.)
			acc_new = rho * acc + (1 - rho) * gparam ** 2
			gradient_scaling = T.sqrt(acc_new + epsilon)
			gparam = gparam / gradient_scaling
			updates.append((acc, acc_new))
			updates.append((param, param - gparam * learnrate))
		return updates
