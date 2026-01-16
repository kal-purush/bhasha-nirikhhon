import numpy as np
import scipy.linalg as la
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.linear_model import LogisticRegression
import pickle


def Raw2Epoch(data,trigger,srate):
    
    epochNUM  = len(trigger)
    epoch = []
    t_min,t_max= -0.2,1

    for epochINX in range(epochNUM):
        data_temp = data[:,int(trigger[epochINX]+t_min*srate):int(trigger[epochINX]+t_max*srate)]
        epoch.append(data_temp)
    
    epoch = np.stack(epoch[i] for i in range(len(epoch)))

    return epoch

def train_TRCA(eegdata,labels):
    """
    Input:
        eeg         : Input eeg data 
                        (# of targets, # of channels, Data length [sample],#of blocks)
        srate          : Sampling rate
        num_fbs     : # of sub-bands
        labels      : labels correspond to train data
    Output:
        model       : Learning model for tesing phase of the ensemble TRCA-based method
            - traindata : Training data decomposed into sub-band components 
            by the filter bank analysis(# of targets, # of sub-bands, # of channels, 
                      Data length [sample])
            - W : Weight coefficients for electrodes which can be 
                     used as a spatial filter.
    """
    ConditionNUM = len(np.unique(labels))
    _,ChannelNUM,Time = eegdata.shape
    # 应对标签缺失的问题

    template = np.zeros((ConditionNUM,ChannelNUM,Time))
    Weight = np.zeros((ConditionNUM,ChannelNUM,ChannelNUM))
    
    for ConditionInx in range(ConditionNUM):
        eeg_temp = np.squeeze(eegdata[labels==ConditionInx,:,:])
        template[ConditionInx,:,:] = np.squeeze(np.mean(eeg_temp,axis=0))
        eeg_temp = np.transpose(eeg_temp,axes=(1,2,0))
        w_tmp =trca(eeg_temp)
        Weight[ConditionInx,:,:] = w_tmp
           
    # save model 
    model = dict(
        Weight=Weight,
        template=template,
    )
    return model

def test_TRCA(test_data,model):
    """
    Input:
        eeg         : Input eeg data 
                        (# of targets, # of channels, Data length [sample])
        model       : Learning model for tesing phase of the ensemble TRCA-based method
            - traindata : Training data decomposed into sub-band components 
            by the filter bank analysis(# of targets, # of sub-bands, # of channels, 
                      Data length [sample])
            - W : Weight coefficients for electrodes which can be 
                     used as a spatial filter.
    Output:
        result,rho

    """
    Weight = model['Weight']
    template = model['template']

    classNUM = 2
    componentNUM = 3
    result = np.zeros((test_data.shape[0]))
    
    for sampleInx in range(test_data.shape[0]):
        test_temp = np.squeeze(test_data[sampleInx,:,:])
        r = np.zeros((classNUM,componentNUM))
        for componentINX in range(componentNUM):
            for classInx in range(classNUM):
                train_template = np.squeeze(template[classInx,:,:])
                w = np.squeeze(Weight[classInx,:,componentINX])
                w = np.expand_dims(w,axis=1)
                rtemp = np.corrcoef(np.dot(test_temp.T,w).T,np.dot(train_template.T,w).T)
                r[classInx,componentINX] = rtemp[0,1]
        r = np.mean(r,axis=-1)
        result[sampleInx] = np.argmax(r)

    return result

def trca(eeg):
    """
    Input:
        eeg : Input eeg data (# of targets, # of channels, Data length [sample])
    Output:
        W : Weight coefficients for electrodes which can be used as a spatial filter.           
    """
    ChannelNUM,Time,TrialNUM = eeg.shape
    S = np.zeros((ChannelNUM,ChannelNUM))
    
    for trial_i in range(TrialNUM):
        x1 = np.squeeze(eeg[:,:,trial_i])
        x1 = x1 - np.mean(x1,axis=1,keepdims=True)
        for trial_j in range(trial_i+1,TrialNUM):
            x2 = np.squeeze(eeg[:,:,trial_j])
            x2 = x2 - np.mean(x2,axis=1,keepdims=True)
            S = S + np.dot(x1,x2.T)+ np.dot(x2,x1.T)
    
    UX = np.stack(eeg[i].ravel() for i in range(len(eeg)))
    UX = UX - np.mean(UX,axis=1,keepdims=True)
    Q = np.dot(UX,UX.T)

    _,W = la.eig(S,Q)
    return W

# CSP
def CSPspatialFilter(Ra,Rb):
    # Input: Ra,Rb are corvariance matrixs of two classes of data
	R = Ra + Rb
	E,U = la.eig(R)

	# CSP requires the eigenvalues E and eigenvector U be sorted in descending order
	ord = np.argsort(E)
	ord = ord[::-1] # argsort gives ascending order, flip to get descending
	E = E[ord]
	U = U[:,ord]

	# Find the whitening transformation matrix
	P = np.dot(np.sqrt(la.inv(np.diag(E))),np.transpose(U))

	# The mean covariance matrices may now be transformed
	Sa = np.dot(P,np.dot(Ra,np.transpose(P)))
	Sb = np.dot(P,np.dot(Rb,np.transpose(P)))

	# Find and sort the generalized eigenvalues and eigenvector
	E1,U1 = la.eig(Sa,Sb)
	ord1 = np.argsort(E1)
	ord1 = ord1[::-1]
	E1 = E1[ord1]
	U1 = U1[:,ord1]

	# The projection matrix (the spatial filter) may now be obtained
	SFa = np.dot(np.transpose(U1),P)

	return SFa.astype(np.float32)

# covarianceMatrix takes a matrix A and returns the covariance matrix, scaled by the variance
def covarianceMatrix(A):
	Ca = np.dot(A,np.transpose(A))/np.trace(np.dot(A,np.transpose(A)))
	return Ca

def CSP(data,label):

    filters = ()
    # number of classes    
    
    # for classINx in range(0,taskNUM):
        # compute covarianceMatrix for class 0
        # initial
    tasks = np.unique(label)
    iterator = range(0,len(tasks))
    for classINx in iterator:
        class_label = label==classINx
        # compute covarianceMatrix for class 1
        R0 = covarianceMatrix(data[class_label][0])
        for t in range(1,len(data[class_label])):
            R0 += covarianceMatrix(data[class_label][t])
        R0 = R0 / len(data[class_label])
        # compute covarianceMatrix for class 1
        R1 = covarianceMatrix(data[~class_label][1])
        for t in range(1,len(data[~class_label])):
            R1 += covarianceMatrix(data[~class_label][t])
        R1 = R1 / len(data[~class_label])

        SF0 = CSPspatialFilter(R0,R1)
        filters += (SF0,)

        if len(tasks) == 2:
            filters += (CSPspatialFilter(R1,R0),)
            break

    return filters

# HDCA
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.linear_model import LogisticRegression

def trainHDCA(data,label):
    """
    
    """
    windowNUM = 6
    # 1. 数据分段
    blockNUM,channelNUM,SampleNUM = np.shape(data)
    
    # reshape data two splitNUM time_windows
    data = np.reshape(data,(blockNUM,channelNUM,windowNUM,int(SampleNUM/windowNUM)))
    reducted_features = ()
    W_fisher = ()
    # 2.fisher
    for windowInx in range(windowNUM):
        data_bin =  np.squeeze(data[:,:,windowInx,:])
        data_bin_ave = np.mean(data_bin,axis=-1)
        fisher= LinearDiscriminantAnalysis()
        reducted_data = fisher.fit_transform(data_bin_ave,label)
        fisher_model = pickle.dumps(fisher)
        W_fisher += (fisher_model,)
        reducted_features += (reducted_data,)

    reducted_features = np.array(np.squeeze(reducted_features)).T

    # 3.Logistic回归
    lgr = LogisticRegression()
    lgr.fit(reducted_features,label)
    lg_model = pickle.dumps(lgr)
    cofficient = dict(
        W_fisher = W_fisher,
        W_logistic = lg_model,
    )

    return  cofficient

def testHDCA(data,model):
    """
    
    """
    windowNUM = 6
    # 1. 数据分段
    blockNUM,channelNUM,SampleNUM = np.shape(data)

    fisher_model = model['W_fisher']
    lg_model = model['W_logistic']

    # reshape data two splitNUM time_windows
    data = np.reshape(data,(blockNUM,channelNUM,windowNUM,int(SampleNUM/windowNUM)))
    reducted_features = ()
    # 2.fisher
    for windowInx in range(windowNUM):
        data_bin =  np.squeeze(data[:,:,windowInx,:])
        data_bin_ave = np.mean(data_bin,axis=-1)

        fisher= pickle.loads(fisher_model[windowInx])
        reducted_data = fisher.transform(data_bin_ave)
        reducted_features += (reducted_data,)

    reducted_features = np.array(np.squeeze(reducted_features)).T
    # 3.Logistic回归
    lgr = pickle.loads(lg_model)
    result = lgr.predict(reducted_features)

    return  result

def Xdawn(data,label):
    """
    docstring
    """

    # 1.epoch 数据转换为pesudo连续数据

    #  Reconstruct pseudo continuous signal from epochs
    X_continous = np.hstack(data)
    _,n_samples = np.shape(X_continous) 

    # define parameters
    srate = 250
    n_min,_ = -0.2*srate,1*srate
    window = 1*srate # window is set to be the length of ERP component 
    toeplitz = list()
    # 2. 根据连续trigger构建toeplitz矩阵
    classes = np.unique(label)
    for _, this_class in enumerate(classes):
        # select events by type
        
        sel = np.argwhere(label == this_class)
        
        # build toeplitz matrix,trig is defined as the overall event of entire recording 
        trig = np.zeros((n_samples, 1))
        ix_trig = sel*srate + n_min
        trig[ix_trig.astype(int)] = 1
        toeplitz.append(la.toeplitz(trig[0:window], trig))

    # Concatenate toeplitz
    toeplitzs = np.array(toeplitz)
    X = np.concatenate(toeplitzs)
    # 3. 根据toeplitz矩阵和连续数据的SVD分解求空域滤波
    predictor = np.dot(la.pinv(np.dot(X, X.T)), X)
    evokeds = np.dot(predictor,X_continous.T)
    evokeds = np.transpose(np.vsplit(evokeds, len(classes)), (0, 2, 1))

    filters = list() #spatial filters
    patterns = list() #patterns filtered by filters

    signal_cov = covarianceMatrix(X_continous)
    n_components = 2 # number of component to keep

    for evo, toeplitz in zip(evokeds, toeplitzs):
        # Estimate covariance matrix of the prototype response
        evo = np.dot(evo, toeplitz)
        evo_cov = covarianceMatrix(evo)
        # fit spatial filters
        evals, evecs = la.eigh(evo_cov, signal_cov) #generalized eigenvalue problem 
        evecs = evecs[:, np.argsort(evals)[::-1]]  # sort eigenvectors
        evecs /= np.apply_along_axis(np.linalg.norm, 0, evecs)
        # pattern is not filter, we can perform pesudo-inverse to filter to get pattern
        _patterns = np.linalg.pinv(evecs.T)
        filters.append(evecs[:, :n_components].T)
        patterns.append(_patterns[:, :n_components].T)

    filters = np.concatenate(filters, axis=0)
    # here we chose not to return patterns and evokeds
    # patterns = np.concatenate(patterns, axis=0)
    # evokeds = np.array(evokeds)
        
    return filters