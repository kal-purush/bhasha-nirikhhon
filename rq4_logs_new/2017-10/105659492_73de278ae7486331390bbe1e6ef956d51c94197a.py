"""Softmax."""


import numpy as np

class SoftMax():


    def softmax(self, x):
        # """Compute softmax values for each sets of scores in x."""
        return np.exp(x) / np.sum(np.exp(x), axis=0)



if __name__ == '__main__':
    scores = [3.0, 1.0, 0.2]
    #scores = [1.0, 2.0, 3.0]
    sm = SoftMax()
    print(sm.softmax(scores).T)

# Plot softmax curves

# juypeter notebook
#import matplotlib.pyplot as plt
# x = np.arange(-2.0, 6.0, 0.1)
# x = np.arange(-1.0,2,0.1)
# scores_ = self.getVstack(x)
# r = softmax(scores_)
# plt.plot(x, softmax(scores).T, linewidth=2)
# plt.show()