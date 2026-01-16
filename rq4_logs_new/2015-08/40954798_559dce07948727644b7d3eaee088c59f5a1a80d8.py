from numpy import array
from numpy import sum as m_sum
from numpy import inf
from sklearn.linear_model import LinearRegression

# initialize data
x = array([0.67,  0.84,  0.6,  0.18,  0.85,  0.47,  1.1,  0.65,  0.36])
y = array([0.61,  0.93,  0.83,  0.35,  0.54,  0.16,  0.91,  0.62,  0.62])
# build data types for fitting model
x_fit = [[o] for o in x]
y_fit = y

# initialize built-in linear model
lin_fit = LinearRegression()
fit = lin_fit.fit(x_fit, y_fit)


# gradient decent algorithm
def cost_fun(y_, y_hat_):
    return m_sum((y_ - y_hat_)**2) / (2 * len(y_))


def predict_fun(beta0_, beta1_, x_):
    return beta0_ + beta1_ * x_


def gradient_beta0(y_, y_hat_):
    return m_sum(y_hat_ - y_)/len(y_)


def gradient_beta1(y_, y_hat_, x_):
    return m_sum((y_hat_ - y) * x_)/len(y_)


beta0 = 0.01
beta1 = 0.01
learning_rate = 0.9

new_cost = inf
old_cost = -inf
threshold = 0.000000001

for i in range(500):
    if abs(new_cost - old_cost) < threshold:
        print "The iteration has stopped at %d'th iteration." % (i+1)
        print "value of beta0 is %f and value of beta1 is %f" % (beta0, beta1)
        print
        print "Analytical solution gives following results."
        print "intercept:", fit.intercept_
        print "coefficient:", fit.coef_
        break
    old_cost = new_cost

    predicted_value = predict_fun(beta0, beta1, x)
    temp0 = beta0 - learning_rate * \
        gradient_beta0(y, predicted_value)
    temp1 = beta1 - learning_rate * \
        gradient_beta1(y, predicted_value, x)

    # update coefficient
    beta0 = temp0
    beta1 = temp1
    new_cost = cost_fun(y, predict_fun(beta0, beta1, x))
    print "iteration number: %d. Cost now is %f." % (i, new_cost)
