import numpy as np

def MSE (w, b, features, targets):
    m = targets.size
    error_quad_sum = 0
    for i in range(m):
        f_x = w * features[i] + b
        error = (f_x - targets[i])**2
        error_quad_sum += error
    mse = error_quad_sum / (2*m)
    return mse

def gradient_derivatives(w, b, features, targets):
    m = targets.size
    error_sum_w = 0
    error_sum_b = 0
    for i in range(m):
        error_w = (w*features[i]+b - targets[i])*features[i]
        error_b = (w*features[i]+b - targets[i])
        error_sum_w += error_w
        error_sum_b += error_b
    
    return ((error_sum_w / m), (error_sum_b / m))


def gradient_descent (gradint_f, features, targets, w_init=0, b_init=0, iter_num=10000, alpha=0.01):
    m = features.size
    w = w_init
    b = b_init

    for iter in range(iter_num):
        (w_d, b_d) = gradint_f(w, b, features, targets)
        w = w - alpha * (w_d)
        b = b - alpha * (b_d)

    return (w,b)


features = np.array([1, 2])
targets = np.array([300.0, 500.0])

(w, b) = gradient_descent(gradient_derivatives, features, targets)
print ("Predict for 1.5: ", w*1.5+b)
print(MSE(200,100,features,targets))