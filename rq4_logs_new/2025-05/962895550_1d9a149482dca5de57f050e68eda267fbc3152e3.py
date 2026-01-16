import pandas as pd
from sklearn.inspection import permutation_importance

def extract_importances(model, X_test, y_test):
    result = permutation_importance( # for each feature, shuffle its values and predict on testing set. do this 10 times for a num_features x 10 collection of drops in MAE
        model, X_test, y_test,
        n_repeats=10, random_state=25,
        scoring='neg_mean_absolute_error'
    )

    importance_df = pd.DataFrame({ # get the mean drop in MAE for each feature and place it next to its feature name in a dataframe
        'feature': X_test.columns,
        'importance': result.importances_mean
    }).sort_values('importance', ascending=False)