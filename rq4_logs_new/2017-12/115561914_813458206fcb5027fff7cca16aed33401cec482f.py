import numpy as np
from sklearn.cluster import KMeans as km
import sklearn.metrics as met
import math as math

syn_input_data = np.loadtxt('input.csv', delimiter=',')
syn_output_data = np.loadtxt('output.csv', delimiter=',').reshape([-1, 1])
letor_input_data = np.loadtxt('Querylevelnorm_X.csv', delimiter=',')
letor_output_data = np.loadtxt('Querylevelnorm_t.csv', delimiter=',').reshape([-1, 1])

#*****Partitioning into tranong, validation and test*******
#*****Synthetic*****

training_set_ip_syn = syn_input_data[0:16000]
validation_set_ip_syn = syn_input_data[16001:18000]
test_set_ip_syn = syn_input_data[18001:20000]

training_set_op_syn = syn_output_data[0:16000]
validation_set_op_syn = syn_output_data[16001:18000]
test_set_op_syn = syn_output_data[18001:20000]

#******Letor*******
training_set_ip_letor = letor_input_data[0:16000]
validation_set_ip_letor = letor_input_data[16001:18000]
test_set_ip_letor = letor_input_data[18001:20000]

training_set_op_letor = letor_output_data[0:16000]
validation_set_op_letor = letor_output_data[16001:18000]
test_set_op_letor = letor_output_data[18001:20000]

#**************Defining Hyper Parameters**************

cluster_size = 10
learning_rate = 0.001
minibatch_size = len(training_set_ip_syn)
num_epochs = 1000
L2_lambda = 0.01

#****************FUNCTIONS***************

def choose_center(X):
    X_kmeans = km(n_clusters = 10, random_state = 0).fit(X)
    X_centers = X_kmeans.cluster_centers_
    return X_centers

def find_spread(X,cluster_size):
    X_kmeans = km(cluster_size, random_state = 0).fit(X)
    X_kmeans_list = {i: X[np.where(X_kmeans.labels_ == i)] for i in range(cluster_size)}
    X_spread = list(np.diag(np.diag(np.cov(X_kmeans_list[i].T))) for i in range(cluster_size))
    return X_spread

def compute_design_matrix(X, centers, spreads):
    centers_new = centers[:,None]
    basis_func_output = np.exp(np.sum(np.matmul(X-centers_new,spreads)*(X-centers_new),axis = 2)/(-2)).T
    return np.insert(basis_func_output,0,1,axis = 1)

def closed_form_sol(L2_lambda, design_matrix, output_data):
    return np.linalg.solve(L2_lambda*np.identity(design_matrix.shape[1])+
                           np.matmul(design_matrix.T,design_matrix),
                           np.matmul(design_matrix.T,output_data)).flatten()

def SGD_sol(learning_rate, minibatch_size, num_epochs, L2_lambda, design_matrix, output_data):
    N,_ = design_matrix.shape
    weights = np.zeros([1,11])
    for epoch in range (num_epochs):
        for i in range (int(N/minibatch_size)):
            lower_bound = i*minibatch_size
            upper_bound = min((i+1)*minibatch_size, N)
            Phi = design_matrix[lower_bound:upper_bound,:]
            t = output_data[lower_bound:upper_bound,:]
            E_D = np.matmul((np.matmul(Phi, weights.T)-t).T,Phi)
            E = (E_D+L2_lambda*weights)/minibatch_size
            weights = weights-learning_rate*E
            #print (np.linalg.norm(E))       
    return weights.flatten()

'''
def early_stopping(learning_rate, minibatch_size, num_epochs, L2_lambda, design_matrix, output_data):
    N,_ = design_matrix.shape
    weights = np.zeros([1,11])
    previous_error = -500
    for epoch in range (num_epochs):
        for i in range (int(N/minibatch_size)):
            lower_bound = i*minibatch_size
            upper_bound = min((i+1)*minibatch_size, N)
            Phi = design_matrix[lower_bound:upper_bound,:]
            t = output_data[lower_bound:upper_bound,:]
            E_D = np.matmul((np.matmul(Phi, weights.T)-t).T,Phi)
            E = (E_D+L2_lambda*weights)/minibatch_size
            weights = weights-learning_rate*E
    for data in range (N):
        predicted = find_predicted_value(weights, design_matrix)
        error = err_func(output_data, predicted, training_set_ip_syn)
    return weights.flatten()
'''

def find_predicted_value(weight, X):
    return np.matmul(X, weight.reshape(-1,1))

def err_func(target, predicted, dataset):
    error = met.mean_squared_error(target, predicted)
    return (math.sqrt((2*error)/(len(dataset))))
    
#*****************END OF BLOCK************************
#****************SYNTHETIC CLOSED FORM*********************

training_set_ip_centers_syn = choose_center(training_set_ip_syn)
tranining_set_ip_spread_syn = find_spread(training_set_ip_syn, cluster_size)
training_set_ip_design_matrix_syn = compute_design_matrix(training_set_ip_syn, training_set_ip_centers_syn, tranining_set_ip_spread_syn)
training_set_ip_closed_syn = closed_form_sol(L2_lambda, training_set_ip_design_matrix_syn, training_set_op_syn)
training_set_ip_predicted_value_syn = find_predicted_value(training_set_ip_closed_syn, training_set_ip_design_matrix_syn)
#print('Synthetic/Closed: Training set error:')
#err_func(training_set_op_syn, training_set_ip_predicted_value_syn, training_set_ip_letor)

validation_set_ip_centers_syn = choose_center(validation_set_ip_syn)
validation_set_ip_spread_syn = find_spread(validation_set_ip_syn, cluster_size)
validation_set_ip_design_matrix_syn = compute_design_matrix(validation_set_ip_syn, validation_set_ip_centers_syn, validation_set_ip_spread_syn)
validation_set_ip_closed_syn = closed_form_sol(L2_lambda, validation_set_ip_design_matrix_syn, validation_set_op_syn)
validation_set_predicted_value_syn = find_predicted_value(validation_set_ip_closed_syn, validation_set_ip_design_matrix_syn)
#print('Synthetic/Closed: Validation set error:')
#err_func(validation_set_op_syn, validation_set_predicted_value_syn, validation_set_ip_syn)

test_set_ip_centers_syn = choose_center(test_set_ip_syn)
test_set_ip_spread_syn = find_spread(test_set_ip_syn, cluster_size)
test_set_ip_design_matrix_syn = compute_design_matrix(test_set_ip_syn, test_set_ip_centers_syn, test_set_ip_spread_syn)
print('Synthetic/Closed: Design matrix for test set:')
print(test_set_ip_design_matrix_syn)
test_set_ip_closed_syn = closed_form_sol(L2_lambda, test_set_ip_design_matrix_syn, test_set_op_syn)
print('Synthetic/Closed: Closed form solution for test set:')
print(test_set_ip_closed_syn)
test_set_ip_predicted_value_syn = find_predicted_value(test_set_ip_closed_syn, test_set_ip_design_matrix_syn)
print('Synthetic/Closed: Test set error:', err_func(test_set_op_syn, test_set_ip_predicted_value_syn, test_set_ip_syn))


#*************END OF BLOCK*****************************
#************SYNTHETIC SGD FORM************************

training_set_ip_centers_sgd = choose_center(training_set_ip_syn)
tranining_set_ip_spread_sgd = find_spread(training_set_ip_syn, cluster_size)
training_set_ip_design_matrix_sgd = compute_design_matrix(training_set_ip_syn, training_set_ip_centers_sgd, tranining_set_ip_spread_sgd)
training_set_ip_sgd = SGD_sol(learning_rate, minibatch_size, num_epochs, L2_lambda, training_set_ip_design_matrix_sgd, training_set_op_syn)
training_set_ip_predicted_value_sgd = find_predicted_value(training_set_ip_sgd, training_set_ip_design_matrix_sgd)
#print('Synthetic/SGD: Training set error:')
#err_func(training_set_op_syn, training_set_ip_predicted_value_sgd, training_set_ip_syn)

validation_set_ip_centers_sgd = choose_center(validation_set_ip_syn)
validation_set_ip_spread_sgd = find_spread(validation_set_ip_syn, cluster_size)
validation_set_ip_design_matrix_sgd = compute_design_matrix(validation_set_ip_syn, validation_set_ip_centers_sgd, validation_set_ip_spread_sgd)
validation_set_ip_sgd = SGD_sol(learning_rate, minibatch_size, num_epochs, L2_lambda, validation_set_ip_design_matrix_sgd, validation_set_op_syn)
validation_set_predicted_value_sgd = find_predicted_value(validation_set_ip_sgd, validation_set_ip_design_matrix_sgd)
#print('Synthetic/SGD: Validation set error:')
#err_func(validation_set_op_syn, validation_set_predicted_value_sgd, validation_set_ip_syn)

test_set_ip_centers_sgd = choose_center(test_set_ip_syn)
test_set_ip_spread_sgd = find_spread(test_set_ip_syn, cluster_size)
test_set_ip_design_matrix_sgd = compute_design_matrix(test_set_ip_syn, test_set_ip_centers_sgd, test_set_ip_spread_sgd)
print('Synthetic/SGD: Design matrix for test set:')
print(test_set_ip_design_matrix_sgd)
test_set_ip_closed_sgd = closed_form_sol(L2_lambda, test_set_ip_design_matrix_sgd, test_set_op_syn)
print('Synthetic/SGD: Closed form solution for test set:')
print(test_set_ip_closed_sgd)
test_set_ip_predicted_value_sgd = find_predicted_value(test_set_ip_closed_sgd, test_set_ip_design_matrix_sgd)
print('Synthetic/SGD: Test set error:', err_func(test_set_op_syn, test_set_ip_predicted_value_sgd, test_set_ip_syn))

#*************END OF BLOCK*****************************
#*******************LETOR CLOSED FORM*********************

training_set_ip_centers_letor = choose_center(letor_input_data)
tranining_set_ip_spread_letor = find_spread(training_set_ip_letor, cluster_size)
training_set_ip_design_matrix_letor = compute_design_matrix(training_set_ip_letor, training_set_ip_centers_letor, tranining_set_ip_spread_letor)
training_set_ip_closed_letor = closed_form_sol(L2_lambda, training_set_ip_design_matrix_letor, training_set_op_letor)
training_set_ip_predicted_value_letor = find_predicted_value(training_set_ip_closed_letor, training_set_ip_design_matrix_letor)
#print('Letor/Closed: Training set error:')
#err_func(training_set_op_letor, training_set_ip_predicted_value_letor, training_set_ip_letor)

validation_set_ip_centers_letor = choose_center(validation_set_ip_letor)
validation_set_ip_spread_letor = find_spread(validation_set_ip_letor, cluster_size)
validation_set_ip_design_matrix_letor = compute_design_matrix(validation_set_ip_letor, validation_set_ip_centers_letor, validation_set_ip_spread_letor)
validation_set_ip_closed_letor = closed_form_sol(L2_lambda, validation_set_ip_design_matrix_letor, validation_set_op_letor)
validation_set_predicted_value_letor = find_predicted_value(validation_set_ip_closed_letor, validation_set_ip_design_matrix_letor)
#print('Letor/Closed: Validation set error:')
#err_func(validation_set_op_letor, validation_set_predicted_value_letor, validation_set_ip_letor)

test_set_ip_centers_letor = choose_center(test_set_ip_letor)
test_set_ip_spread_letor = find_spread(test_set_ip_letor, cluster_size)
test_set_ip_design_matrix_letor = compute_design_matrix(test_set_ip_letor, test_set_ip_centers_letor, test_set_ip_spread_letor)
print('Letor/Closed: Design matrix for test set:')
print(test_set_ip_design_matrix_sgd)
test_set_ip_closed_letor = closed_form_sol(L2_lambda, test_set_ip_design_matrix_letor, test_set_op_letor)
print('Letor/Closed: Closed form solution for test set:')
print(test_set_ip_closed_letor)
test_set_ip_predicted_value_letor = find_predicted_value(test_set_ip_closed_letor, test_set_ip_design_matrix_letor)
print('Letor/Closed: Test set error:', err_func(test_set_op_letor, test_set_ip_predicted_value_letor, test_set_ip_letor))

#*****************END OF BLOCK**************************
#*****************LETOR SGD FORM************************

training_set_ip_centers = choose_center(training_set_ip_letor)
tranining_set_ip_spread = find_spread(training_set_ip_letor, cluster_size)
training_set_ip_design_matrix = compute_design_matrix(training_set_ip_letor, training_set_ip_centers, tranining_set_ip_spread)
training_set_ip_sgd = SGD_sol(learning_rate, minibatch_size, num_epochs, L2_lambda, training_set_ip_design_matrix, training_set_op_letor)
training_set_ip_predicted_value = find_predicted_value(training_set_ip_sgd, training_set_ip_design_matrix)
#print('Letor/SGD: Training set error:')
#err_func(training_set_op_letor, training_set_ip_predicted_value, training_set_ip_letor)

validation_set_ip_centers = choose_center(validation_set_ip_letor)
validation_set_ip_spread = find_spread(validation_set_ip_letor, cluster_size)
validation_set_ip_design_matrix = compute_design_matrix(validation_set_ip_letor, validation_set_ip_centers, validation_set_ip_spread)
validation_set_ip_sgd = SGD_sol(learning_rate, minibatch_size, num_epochs, L2_lambda, validation_set_ip_design_matrix, validation_set_op_letor)
validation_set_predicted_value = find_predicted_value(validation_set_ip_sgd, validation_set_ip_design_matrix)
#print('Letor/SGD: Validation set error:')
#err_func(validation_set_op_letor, validation_set_predicted_value, validation_set_ip_letor)

test_set_ip_centers_letor_sgd = choose_center(test_set_ip_letor)
test_set_ip_spread_letor_sgd = find_spread(test_set_ip_letor, cluster_size)
test_set_ip_design_matrix_letor_sgd = compute_design_matrix(test_set_ip_letor, test_set_ip_centers_letor_sgd, test_set_ip_spread_letor_sgd)
print('Letor/SGD: Design matrix for test set:')
print(test_set_ip_design_matrix_letor_sgd)
test_set_ip_closed_letor_sgd = closed_form_sol(L2_lambda, test_set_ip_design_matrix_letor_sgd, test_set_op_letor)
print('Letor/SGD: Closed form solution for test set:')
print(test_set_ip_closed_letor_sgd)
test_set_ip_predicted_value_letor_sgd = find_predicted_value(test_set_ip_closed_letor_sgd, test_set_ip_design_matrix_letor_sgd)
print('Letor/SGD: Test set error:', err_func(test_set_op_letor, test_set_ip_predicted_value_letor_sgd, test_set_ip_letor))

#*****************END OF BLOCK**************************
