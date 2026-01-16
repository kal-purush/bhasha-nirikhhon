import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb

from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler

import warnings
warnings.filterwarnings("ignore")

def plot_roc(y_true, y_score, label_name, ax):
    fpr, tpr, thresholds = metrics.roc_curve(y_true, y_score)
    ax.plot(fpr, tpr)
    ax.plot([0, 1], [0, 1], color='grey', linestyle='--')
    ax.set_ylabel('TPR')
    ax.set_xlabel('FPR')
    ax.set_title(
        f"{label_name}: AUC = {metrics.roc_auc_score(y_true, y_score):.4f}"
    )
    
def scale_and_split(features, target, scale, scaler=None):
    if scale == True and scaler==None:
        sc = StandardScaler()
        features = sc.fit_transform(features.values)
    elif scale == True and scaler=="MinMax":
        mm_scaler = MinMaxScaler()
        feature = mm_scaler.fit_transform(features.values)
        
    
    X_train, X_eval, y_train, y_eval = train_test_split(
                                                        features,
                                                        target,
                                                        test_size=0.20,
                                                        shuffle=True,
                                                        stratify=target,
                                                        random_state=0
                                                        )
    return X_train, X_eval, y_train, y_eval
        

def fit_and_estimate(features,
                     target,
                     estimator, 
                     label=None, 
                     multiLabel=None,
                     scale=True,
                     scaler=None):
    
    X_train, X_eval, y_train, y_eval = scale_and_split(features, target, scale, scaler)    

    estimator.fit(X_train, y_train)
    
    if multiLabel == True:
        preds = estimator.predict_proba(X_eval)
        y_preds = pd.DataFrame(
            {
                "h1n1_vaccine": preds[0][:, 1],
                "seasonal_vaccine": preds[1][:, 1],
            },
            index = y_eval.index
        )
        
        fig, ax = plt.subplots(1, 2, figsize=(7, 3.5))

        plot_roc(
            y_eval['h1n1_vaccine'], 
            y_preds['h1n1_vaccine'], 
                    'h1n1_vaccine',
            ax=ax[0]
            )
        plot_roc(
            y_eval['seasonal_vaccine'], 
            y_preds['seasonal_vaccine'], 
            'seasonal_vaccine',
            ax=ax[1]
        )
        fig.tight_layout()
    
    else:
        if label == 'h1n1_vaccine':
            h1n1_preds = estimator.predict_proba(X_eval)
        
            y_preds = pd.DataFrame(
                {
                "h1n1_vaccine": h1n1_preds[:, 1]
                },
                index = y_eval.index
            )
    
            fig, ax = plt.subplots(1, 1, figsize=(7, 3.5))

            plot_roc(
                y_eval['h1n1_vaccine'], 
                y_preds['h1n1_vaccine'], 
                        'h1n1_vaccine',
                ax=ax
                )
        elif label == 'seasonal_vaccine':
            seasonal_preds = estimator.predict_proba(X_eval)
        
            y_preds = pd.DataFrame(
                {
                    "seasonal_vaccine": seasonal_preds[:, 1]
                },
                index = y_eval.index
            )
    
            fig, ax = plt.subplots(1, 1, figsize=(7, 3.5))

            plot_roc(
                y_eval['seasonal_vaccine'], 
                y_preds['seasonal_vaccine'], 
                        'seasonal_vaccine',
                ax=ax
            )

        return fig.tight_layout()
    