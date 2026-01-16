# general python library imports
from typing import Dict, List
# from xgboost import XGBClassifier
import logging
import joblib
import json
import click
import os
import uuid

# core physarum library imports

import physarum
from physarum.phyml import sklearn_node, sklearn_pipeline
from physarum import phyml
from physarum.features import one_hot_encode
from physarum.features import get_unique_feature_values, describe_features

from physarum.features import one_hot_encode
from physarum.features import get_unique_feature_values, describe_features

from sklearnPipeline import execute
from sklearnPipeline import (LogisticRegression,
                            SelectFromModel,
                            SelectKBest,
                            DataEncoder,
                            SimpleImputer,
                            ContinuousColumnSelector,
                            CategoricalColumnSelector)

# application specific imports
# from customerapp.shared import version_params 
# from customerapp.shared import data, version_params 

from customerapp.shared import MODEL_NAME
from customerapp.shared import FEATURES_NUMERIC, FEATURES_CATEGORICAL, TARGET

@phyml.op()
def select_into(sql: str, output_dataset: str, output_table: str):
    # a kind fo hack as the initilaizing service was causing error (to be fixed later)
    # we can also pass it using phyml.pass_context, as essentially we need only the
    # services from the package
    pkg = phyml.get_package()

    services = pkg.services
    services.warehouse.select_into(sql, output_dataset, output_table)
    return output_table


@phyml.op()
def export_csv(bucket: str, dataset_name: str, table_name: str):
    pkg = phyml.get_package()

    services = pkg.services
    bucket_path = pkg.artifact_path(table_name + ".csv")
    bucket_url = services.warehouse.export_csv(bucket, bucket_path, dataset_name, table_name)
    return bucket_path

@phyml.op()
def analyze_categorical_features(bucket: str, csv_path: str, artifact_name: str, columns: List[str]):
    
    pkg = phyml.get_package()
    pkg_data = pkg.get()

    services = pkg.services

    training_df = services.lake.download_csv(bucket, csv_path)
    # print("training df is obtained")
    unique_feature_values = get_unique_feature_values(training_df, columns)
    # Add the artifact we have just produced
    return pkg.add_artifact_json(artifact_name, unique_feature_values)


@phyml.op()
def analyze_numeric_features(bucket: str, csv_path: str, artifact_name: str, columns: List[str]
                             ):
    pkg = phyml.get_package()
    services = pkg.services

    training_df = services.lake.download_csv(bucket, csv_path)
    unique_feature_values = describe_features(training_df, columns)

    # Add the artifact we have just produced
    return pkg.add_artifact_json(artifact_name, unique_feature_values)

@phyml.op()
def build_matrix(bucket: str, csv_path: str, analysis_path_categorical: str, numeric_features: List[str], target: str, artifact_name: str):
    pkg = phyml.get_package()
    services = pkg.services
    json_features = services.lake.download_string(bucket, analysis_path_categorical)
    unique_feature_values = json.loads(json_features)
    training_df = services.lake.download_csv(bucket, csv_path)

    # Add in our categorical features via one-hot encoding
    encoded_df = one_hot_encode(training_df, unique_feature_values, throw_on_missing=True)
    # Add in all the numeric columns
    for nf in numeric_features:
        encoded_df[nf] = training_df[nf]

    # Add in the target column to our new encoded data frame
    encoded_df[target] = training_df[target]

    # Add the encoded dataframe as an artifact
    return pkg.add_artifact_dataframe(artifact_name, encoded_df)


# @phyml.op()
# def churn_pipeline_op1(bucket: str,
#                    train_path: str,
#                    test_path: str,
#                    output_path: str,
#                    class_column: str,
#                    test_ratio=0.3,
#                    version_params = None,
#                    artifact_name = None):

#     # will be provide by feature store
#     pkg = phyml.get_package()
#     services = pkg.services
    
#     # reolving the bucket_path to local_path
#     train_path = services.lake.download_csv_local(bucket, train_path)
#     if test_path is not None:
#         test_path = services.lake.download_csv(bucket, test_path)
    
#     # we cannot hardcode names here; will be passed from above 
#     @sklearn_pipeline(name = "mypipeline", description = "sklearn training" , task = "Classification", parameter_tuning = None, parameter_tuning_params = {})
#     def my_pipeline(train_path=train_path, test_path=test_path, output_path=output_path, sep="\t"):
    
#         # cat = sklearn_node(transformer = CategoricalColumnSelector , config=  {"categorical_columns": ["a", "c", "f"] , "unique_counts" : 20}   ) 
#         cat = sklearn_node(transformer = CategoricalColumnSelector , config=  {"unique_counts" : 20}   ) 
#         cont = sklearn_node(transformer = ContinuousColumnSelector, config=  { "continuous_columns" : None, "unique_counts" : 20} )
#         imp = sklearn_node(transformer = SimpleImputer, config=    {  "strategy": "most_frequent"}  ).after(cat)
#         denc = sklearn_node(transformer = DataEncoder, config=    { "columns" : None} ).after(imp)
#         imp2 = sklearn_node(transformer = SimpleImputer, config=   { "strategy": "mean"} ).after(cont)
#         kbest = sklearn_node(transformer = SelectKBest, config=   { "score_func" : "f_regression" ,  "k": "all"} ).after(denc)  
#         kbest2 = sklearn_node(transformer = SelectKBest, config=   { "score_func" : "f_regression" ,  "k": "all" } ).after(imp2)
#         fmodel = sklearn_node(transformer = SelectFromModel, config=   {"estimator" : "LogisticRegression"  , "estimator_params" : {} ,  "threshold" : None } ).after(kbest,kbest2)
#         logg = sklearn_node(transformer = LogisticRegression, config=  {"C":[1 , 3]   ,"tol":[ 0.0001 , 0.001 ] ,"penalty": ["l1" , "l2"]  } ).after(fmodel)

#     # get the dag and execute
#     pipe = my_pipeline()
#     execute(pipe, class_column=class_column, test_ratio=test_ratio)

#     bucket_path = pkg.artifact_folder(output_path)
#     bucket_url = services.lake.copy_local_directory(bucket, bucket_path, output_path)
#     print("data loaded to bucket")
 
#     # artifact_tmp_path = os.path.join(output_path, "model.joblib")
#     return bucket_path

# another way of writing the pipeline

@sklearn_pipeline(name = "mypipeline2", description = "sklearn training one way" , task = "Classification", parameter_tuning = None, parameter_tuning_params = {})
def cltv_pipeline(train_path=None, test_path=None, output_path=None, sep="\t"):
    
    cat = sklearn_node(transformer = CategoricalColumnSelector , config=  {"unique_counts" : 20}   ) 
    # cat = sklearn_node(transformer = CategoricalColumnSelector , config=  {"unique_counts" : 20}   ) 
    cont = sklearn_node(transformer = ContinuousColumnSelector, config=  { "continuous_columns" : None, "unique_counts" : 20} )
    imp = sklearn_node(transformer = SimpleImputer, config=    {  "strategy": "most_frequent"}  ).after(cat)
    denc = sklearn_node(transformer = DataEncoder, config=    { "columns" : None} ).after(imp)
    imp2 = sklearn_node(transformer = SimpleImputer, config=   { "strategy": "mean"} ).after(cont)
    kbest = sklearn_node(transformer = SelectKBest, config=   { "score_func" : "f_regression" ,  "k": "all"} ).after(denc)  
    kbest2 = sklearn_node(transformer = SelectKBest, config=   { "score_func" : "f_regression" ,  "k": "all" } ).after(imp2)
    fmodel = sklearn_node(transformer = SelectFromModel, config=   {"estimator" : "LogisticRegression"  , "estimator_params" : {} ,  "threshold" : None } ).after(kbest,kbest2)
    logg = sklearn_node(transformer = LogisticRegression, config=  {"C":[1 , 3]   ,"tol":[ 0.0001 , 0.001 ] ,"penalty": ["l1" , "l2"]  } ).after(fmodel)
    

# dynamically pick the node based on the type of pipeline
@sklearn_pipeline(name = "mypipeline3", description = "sklearn training another way" , task = "Classification", parameter_tuning = None, parameter_tuning_params = {})
def propensity_pipeline(train_path=None, test_path=None, output_path=None, sep="\t"):
    
    cat = sklearn_node(transformer = CategoricalColumnSelector , config=  {"unique_counts" : 20}   ) 
    cont = sklearn_node(transformer = ContinuousColumnSelector, config=  { "continuous_columns" : None, "unique_counts" : 20} )
    imp = sklearn_node(transformer = SimpleImputer, config=    {  "strategy": "most_frequent"}  ).after(cat)
    denc = sklearn_node(transformer = DataEncoder, config=    { "columns" : None} ).after(imp)
    imp2 = sklearn_node(transformer = SimpleImputer, config=   { "strategy": "mean"} ).after(cont)
    kbest = sklearn_node(transformer = SelectKBest, config=   { "score_func" : "f_regression" ,  "k": "all"} ).after(denc)  
    kbest2 = sklearn_node(transformer = SelectKBest, config=   { "score_func" : "f_regression" ,  "k": "all" } ).after(imp2)
    fmodel = sklearn_node(transformer = SelectFromModel, config=   {"estimator" : "LogisticRegression"  , "estimator_params" : {} ,  "threshold" : None } ).after(kbest,kbest2)
    logg = sklearn_node(transformer = LogisticRegression, config=  {"C":[1 , 3]   ,"tol":[ 0.0001 , 0.001 ] ,"penalty": ["l1" , "l2"]  } ).after(fmodel)

mlpipelines = {"cltv_pipeline": cltv_pipeline,
              "propensity_pipeline": propensity_pipeline}

# In this case we think of Sklearn pipeline as a "SQL query" which can run itself against any data/table/database (tables are parameterized)

@phyml.op()
def sklearn_pipeline_op2(pipeline_name: str,
                             bucket: str,
                             train_path: str,
                             test_path: str,
                             output_path: str,
                             class_column: str,
                             test_ratio,
                             version_params: str,
                             artifact_name: str
                             ):
    
    # resolving the bucket_path to local_path    
    
    pkg = phyml.get_package()
    config = pkg.services.config
    services = pkg.services
    final_df = services.lake.download_csv(bucket, train_path)
    pipe = mlpipelines[pipeline_name]
    
    pipe_dag = pipe(train_path=final_df, test_path=final_df, output_path=output_path)
    execute(pipe_dag, class_column=class_column, test_ratio=test_ratio)
    
    # pkg = phyml.get_package()
    # print(pkg)
    # services = pkg.services
    
    # train_path = services.lake.download_csv_local(bucket, train_path)
    # if test_path is not None:
    #     test_path = services.lake.download_csv_local(bucket, test_path)
    
    # # get the required pipe object
    # pipe = mlpipelines[pipeline_name]
    # # get the dag and execute
    # pipe_dag = pipe(train_path=train_path, test_path=test_path, output_path=output_path)
    # execute(pipe_dag, class_column=class_column, test_ratio=test_ratio)    
    # print("data loaded to bucket")
    
    return pkg.add_artifact_folder(output_path)

    # we can move this code to execute itself (coz the folder structure cannot be arbirtary)
    # order will be required for model_container to effectively work
    
    # bucket_path = pkg.artifact_folder(output_path)
    # bucket_url = services.lake.copy_local_directory(bucket, bucket_path, output_path)
    # print("print the required path")
    # print(bucket)
    # print(bucket_path)
    # print(output_path)
    
    # services.lake.copy_local_directory_to_gcs(bucket_name, bucket_path, local_path)
    # artifact_tmp_path = os.path.join(output_path, "model.joblib")    
    # return bucket_path

@phyml.op()
def sklearn_pipeline_op3(pipeline_name: str,
                             bucket: str,
                             train_path: str,
                             test_path: str,
                             output_path: str,
                             class_column: str,
                             test_ratio=0.3,
                             version_params = None,
                             artifact_name = None):

    # resolving the bucket_path to local_path    
    pkg = phyml.get_package()
    config = pkg.services.config
    services = pkg.services
    final_df = services.lake.download_csv(bucket, train_path)
    pipe = mlpipelines[pipeline_name]
    
    pipe_dag = pipe(train_path=final_df, test_path=final_df, output_path=output_path)
    execute(pipe_dag, class_column=class_column, test_ratio=test_ratio)
    # pkg = phyml.get_package()
    # services = pkg.services
    
    # train_path = services.lake.download_csv_local(bucket, train_path)
    # if test_path is not None:
    #     test_path = services.lake.download_csv_local(bucket, test_path)
    # # get the required pipe object
    # pipe = mlpipelines[pipeline_name]
    
    # # get the dag and execute
    # pipe_dag = pipe(train_path=train_path, test_path=test_path, output_path=output_path)
    # execute(pipe_dag, class_column=class_column, test_ratio=test_ratio)
    
    bucket_path = pkg.artifact_folder(output_path)
    bucket_url = services.lake.copy_local_directory(bucket, bucket_path, output_path)
    print("data loaded to bucket")
 
    # artifact_tmp_path = os.path.join(output_path, "model.joblib")
    return pkg.add_artifact_folder(output_path)

# @phyml.op()
# def custom_model(bucket: str, train_path: str, target: str, artifact_name: str):
#     pkg = phyml.get_package()
#     config = pkg.services.config
#     services = pkg.services
#     final_df = services.lake.download_csv(bucket, train_path)
#     targets = final_df[target]
#     feature_matrix = final_df.values

#     classifier = XGBClassifier()
#     model = classifier.fit(feature_matrix, targets, verbose=True)
    
#     filename = uuid.uuid4()
#     tmp_path = os.path.join("/tmp/", f"{filename}.joblib")
    
#     joblib.dump(model, tmp_path)

#     artifact_path = pkg.add_artifact_file(artifact_name, tmp_path)
#     # os.remove(tmp_path)
#     return artifact_path
    # return bucket_path



# @phyml.op()
# def download_data(bucket: str, bucket_path: str, artifact_name: str):
    
#     pkg = phyml.get_package()
#     pkg_data = pkg.get()    
#     services = pkg.services
    
#     tmp_path = services.lake.download_csv_local(bucket, bucket_path)
#     artifact_path = pkg.add_artifact_file(artifact_name, tmp_path)
    
#     # return artifact_path
#     return tmp_path
    # Or we can do it here
