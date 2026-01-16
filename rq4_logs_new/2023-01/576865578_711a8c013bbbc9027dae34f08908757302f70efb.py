# author: Mohammad Reza Nabizadeh
# date: 2023-01-08

"""

This script runs the a set of ensemble models and 
report the model scores. The models are Random Forest Regressor,
Gradient Boosting Regressor, LGBM Regressor and XGB Regressor. Besides running
the models, it also runs SHAP feature importance model to help the models 
interpretability.

Usage: shap_feat_importances.py --output_dir=<output_dir> --input_file_path=<input_file_path>
 
Options:
--output_dir=<output_dir>
Path to folder where the baseline model results (R2 and Neg Root Mean Squared 
Error will be saved, `in quotes.

--input_file_path=<input_file_path>
Path to folder and file name where the preprocessed file with engineered
features is stored, including the name, `in quotes.

"""

# Import dependencies
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from docopt import docopt
import os
import dataframe_image as dfi
from sklearn.model_selection import train_test_split
from sklearn.ensemble import (
    GradientBoostingRegressor,
    RandomForestRegressor
)
from lightgbm.sklearn import LGBMRegressor
from xgboost import XGBRegressor
from sklearn.pipeline import make_pipeline
from preprocessor import preprocessor
import shap


opt = docopt(__doc__)

def main(output_dir, input_file_path):

    # Read the preprocessed file
    prep_feat_df = pd.read_csv(input_file_path, low_memory=False)

    # Split the file for training and testing the model
    train_df, test_df = train_test_split(prep_feat_df,
                                         test_size=0.9,
                                         random_state=123)
    
    # Form the X and y of the training and test dataframes
    X_train = train_df.drop(columns=['reviews_per_month'])
    y_train_untrans = train_df['reviews_per_month']
    X_test = test_df.drop(columns=['reviews_per_month'])
    y_test_untrans = test_df['reviews_per_month']

    # Transform the target column
    y_train = np.log10(y_train_untrans)
    y_test = np.log10(y_test_untrans)

    # LGBM model
    lgbm_model = LGBMRegressor(random_state=123)
    lgbm_pipeline = make_pipeline(preprocessor(), lgbm_model)
    lgbm_pipeline.fit(X_train, y_train)

    # XGB model
    xgb_model = XGBRegressor(random_state=123)
    xgb_pipeline = make_pipeline(preprocessor(), xgb_model)
    xgb_pipeline.fit(X_train, y_train)

    # Get the list of trnsformed columns for geting the feature importances
    transformer_stage = lgbm_pipeline.named_steps['columntransformer']

    categorical_features_trans = transformer_stage.named_transformers_[
        'onehotencoder-1'
        ].get_feature_names_out()
    binary_features_trans = transformer_stage.named_transformers_[
        'onehotencoder-2'
        ].get_feature_names_out()
    ordinal_features_trans = transformer_stage.named_transformers_[
        'ordinalencoder'
        ].get_feature_names_out()
    numerical_features_trans = transformer_stage.named_transformers_[
        'pipeline'
        ].named_steps[
            'polynomialfeatures'
            ].get_feature_names_out()
    countvec_features_trans_amen = transformer_stage.named_transformers_[
        'countvectorizer'
        ].get_feature_names_out()

    features_transformed = categorical_features_trans.tolist() + \
                                binary_features_trans.tolist() + \
                                ordinal_features_trans.tolist() + \
                                numerical_features_trans.tolist() + \
                                countvec_features_trans_amen.tolist()

    # From the transformed X_train for SHAP explainer
    X_train_enc = pd.DataFrame(
        data=preprocessor().fit_transform(X_train).toarray(),
        columns=features_transformed,
        index=X_train.index,
        )

    # SHAP explainer for LGBM model
    lgbm_explainer = shap.Explainer(lgbm_pipeline.named_steps['lgbmregressor'])
    lgbm_shap_values = lgbm_explainer(X_train_enc)
    fig_shap=plt.gcf()
    shap.summary_plot(lgbm_shap_values,
                      X_train_enc,
                      max_display=20,
                      show=False)

    # Save the results 
    output_shap_sum_plt = output_dir + '/08_shap_sum_plt.png'

    try:
        fig_shap.savefig(output_shap_sum_plt)

    except:
        os.makedirs(os.path.dirname(output_shap_sum_plt))
        fig_shap.savefig(output_shap_sum_plt)

if __name__ == "__main__":
    main(opt["--output_dir"], opt["--input_file_path"])   
    