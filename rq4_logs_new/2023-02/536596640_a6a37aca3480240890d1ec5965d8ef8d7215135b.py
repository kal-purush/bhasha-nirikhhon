import numpy as np

def replicate_classes(X, y):

    X_class_minoritary = X[y==1]
    y_class_minoritary = y[y==1]
    
    conX = np.concatenate((X, X_class_minoritary), axis=0)
    
    conY = np.concatenate((y, y_class_minoritary), axis=0)
    
    return conX, conY