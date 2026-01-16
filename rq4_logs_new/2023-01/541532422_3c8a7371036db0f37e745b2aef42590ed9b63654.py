# -*- coding: utf-8 -*-

# modules
import itertools
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, 'src/si')
from typing import *

from data.dataset import Dataset
from model_selection.cross_validate import cross_validate


def randomized_search_cv(model, dataset: Dataset, parameter_distribution: Dict[str, Tuple], scoring: Callable = None, cv: int = 3, n_iter: int = 10, test_size: float = 0.2) -> List[Dict[str, Any]]:
    """
    Performs a randomized search cross validation on a model.

    Parameters
    ----------
    model
        The model to cross validate.
    dataset: Dataset
        The dataset to cross validate on.
    parameter_distribution: Dict[str, Tuple]
        The parameter distribution to use.
    scoring: Callable
        The scoring function to use.
    cv: int
        The cross validation folds.
    n_iter: int
        The number of random combinations of hyperparameters.
    test_size: float
        The test size.
    """
    for parameter in parameter_distribution: 
        if not hasattr(model, parameter):  # Checks if the given parameters in parameter_distribution exists in the model
            raise AttributeError(f"Model {model} does not haver parameter {parameter}")

    scores = []
    
    for i in range(n_iter):  # for each combination

        parameters = {}    # parameter configuration

        # set the parameters
        for parameter in parameter_distribution: 
            value = np.random.choice(parameter_distribution[parameter])   #take a random value from the value distribution of each parameter
            setattr(model, parameter, value)
            parameters[parameter] = value
        
        score = cross_validate(model=model, dataset=dataset, scoring=scoring, cv=cv, test_size=test_size)
        score['parameters'] = parameters   # add the parameter configuration
        scores.append(score)   # add the score
    
    return scores  # pd.DataFrame(scores)


if __name__ == '__main__':

    print("--------Example 1--------")
    from linear_model.logistic_regression import LogisticRegression
    dataset_ = Dataset.from_random(600, 100, 2)

    # initialize the Logistic Regression model
    knn = LogisticRegression(use_adaptive_alpha=False)
    parameter_distribution_ = {'l2_penalty': np.linspace(1, 10, 10), 'alpha': np.linspace(0.001, 0.0001, 100), 'max_iter': np.linspace(1000, 2000, 200, dtype = int)}

    scores_ = randomized_search_cv(knn, dataset_, parameter_distribution=parameter_distribution_, cv=3)
    print(f"Scores: {scores_}\n")


    print("--------Example 2--------")
    
    sys.path.insert(0, 'src')
    from si.io.csv_file import read_csv
    path = 'C:/Users/ASUS/Desktop/Bioinfo/2ano/Sistemas Inteligentes/si/datasets/breast-bin.csv'
    breast = read_csv(path, sep = ",", features = False, label = True)
    
    from sklearn.preprocessing import StandardScaler
    breast.X = StandardScaler().fit_transform(breast.X) 
    
    log_reg = LogisticRegression(use_adaptive_alpha=False)
    parameter_distribution = {"l2_penalty": np.linspace(1,10,10), "alpha": np.linspace(0.001,0.0001,100), "max_iter": np.linspace(1000,2000,200, dtype = int)}
    scores = randomized_search_cv(log_reg, breast, parameter_distribution, cv=3)
    print(f"Scores: \n{pd.DataFrame(scores)}") 