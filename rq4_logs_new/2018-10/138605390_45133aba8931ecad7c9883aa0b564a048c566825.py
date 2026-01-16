#------------------------------------------------------------------------------------------------------------------------------
# By Alexandra Lee 
# (updated October 2018) 
#
# Define offset vectors
# An offset vector represents the difference in gene expression profiles between two states (ex. two different conditions like # disease vs normal)
# 
#-------------------------------------------------------------------------------------------------------------------------------
import os
import pandas as pd
import numpy as np
from keras.models import model_from_json, load_model
from keras import metrics, optimizers
from functions import utils

from numpy.random import seed
randomState = 123
seed(randomState)

def gene_space_offset(data_dir, gene_id, percent_low, percent_high):
    
    """
    gene_space_offset(data_dir: string, gene_id: string):

    input: 
        data_dir: directory containing the raw gene expression data for all genes including the target gene (see
                    gene_id definition).
        
        gene_id: gene you are using as the "phenotype" to sort samples by 
        
                 This gene is referred to as "target_gene" in comments below.
                 In "interpolate_in_gene_space.py", after we sort samples based on the expression level of the
                 target gene, we want to predict the expression profile of the OTHER genes at different levels
                 of target gene expression.
        
        percent_low: integer between 0 and 1
    
        percent_high: integer between 0 and 1
        
    computation:
        offset_vector = average(gene expression of samples that have the highest 5% of target gene expression) -  
          average(gene expression of samples that have the lowest 5% of target gene expression) 
    
    output:
     offset vector (1 x 5548 genes)     
     Note: offset vector does not include the target gene 
    
    """
    
    # Load arguments
    target_gene_file = os.path.join(data_dir, "PA1673.txt")
    non_target_gene_file = os.path.join(data_dir, "train_model_input.txt.xz")

    # Output files
    offset_file = os.path.join(data_dir, "offset_gene_space.txt")
    lowest_file = os.path.join(data_dir, "lowest.txt")
    highest_file = os.path.join(data_dir, "highest.txt")

    # Read in data
    target_gene_data = pd.read_table(target_gene_file, header=0, index_col=0)
    non_target_gene_data = pd.read_table(non_target_gene_file, header=0, index_col=0)

    # Sort target gene data by expression (lowest --> highest)
    target_gene_sorted = target_gene_data.sort_values(by=[gene_id])

    # Collect the extreme gene expressions
    [low_ids, high_ids] = utils.get_gene_expression_above_percent(target_gene_sorted, gene_id, percent_low, percent_high)
    low_exp = non_target_gene_data.loc[low_ids]
    high_exp = non_target_gene_data.loc[high_ids]

    print('Number of genes in low expression group is {}'.format(low_exp.shape))
    print('Number of gene in high expression group is {}'.format(high_exp.shape))

    # Average gene expression across samples in each extreme group
    lowest_mean = low_exp.mean(axis=0)
    highest_mean = high_exp.mean(axis=0)

    # Generate offset using average gene expression in original dataset
    offset_gene_space = highest_mean - lowest_mean
    offset_gene_space_df = pd.Series.to_frame(offset_gene_space).T

    # output lowest and highest expressing samples
    low_exp.to_csv(lowest_file, sep='\t', float_format="%.5g")
    high_exp.to_csv(highest_file, sep='\t', float_format="%.5g")

    # ouput gene space offset vector
    offset_gene_space_df.to_csv(offset_file, sep='\t', float_format="%.5g")
    

def latent_space_offset(data_dir, model_dir, encoded_dir, gene_id, percent_low, percent_high):
    
    """
    latent_space_offset(data_dir: string, model_dir: string, encoded_dir: string, gene_id: string):

    input: 
        data_dir: directory containing the raw gene expression data for all genes including the target gene (see
                    gene_id definition).
        model_dir: directory containing the learned vae models
        
        encoded_dir: directory to use to output offset vector to
        
        gene_id: gene you are using as the "phenotype" to sort samples by 
        
                 This gene is referred to as "target_gene" in comments below.
                 In "interpolate_in_latent_space.py", after we sort samples based on the expression level of the
                 target gene, we want to predict the expression profile of the OTHER genes at different levels
                 of target gene expression.
        
        percent_low: integer between 0 and 1
    
        percent_high: integer between 0 and 1
    
    computation:
        offset_vector = average(encoded gene expression of samples that have the highest 5% of target gene expression) -  
          average(encoded gene expression of samples that have the lowest 5% of target gene expression) 
    
    output:
        encoded offset vector (1 x number of latent space features)     
        Note: offset vector does not include the target gene 
     
    """
    
    # Load arguments
    target_gene_file = os.path.join(data_dir, "PA1673.txt")
    non_target_gene_file = os.path.join(data_dir, "train_model_input.txt.xz")

    model_file = os.path.join(model_dir, "tybalt_2layer_10latent_encoder_model.h5")
    weights_file = os.path.join(model_dir, "tybalt_2layer_10latent_encoder_weights.h5")

    # Output files
    offset_file = os.path.join(encoded_dir, "offset_latent_space.txt")
    lowest_file = os.path.join(encoded_dir, "lowest_encoded.txt")
    highest_file = os.path.join(encoded_dir, "highest_encoded.txt")

    # Read in data
    target_gene_data = pd.read_table(target_gene_file, header=0, index_col=0)
    non_target_gene_data = pd.read_table(non_target_gene_file, header=0, index_col=0)

    # read in saved models
    loaded_model = load_model(model_file)

    # load weights into new model
    loaded_model.load_weights(weights_file)

    # Sort target gene data by expression (lowest --> highest)
    target_gene_sorted = target_gene_data.sort_values(by=[gene_id])

    # Collect the extreme gene expressions
    [low_ids, high_ids] = utils.get_gene_expression_above_percent(target_gene_sorted, gene_id, percent_low, percent_high)
    low_exp = non_target_gene_data.loc[low_ids]    
    high_exp = non_target_gene_data.loc[high_ids]

    print('Number of genes in low expression group is {}'.format(low_exp.shape))
    print('Number of gene in high expression group is {}'.format(high_exp.shape))

    # Use trained model to encode expression data into SAME latent space
    low_exp_encoded = loaded_model.predict_on_batch(low_exp)
    low_exp_encoded_df = pd.DataFrame(low_exp_encoded, index=low_exp.index)

    high_exp_encoded = loaded_model.predict_on_batch(high_exp)
    high_exp_encoded_df = pd.DataFrame(high_exp_encoded, index=high_exp.index)


    # Average gene expression across samples in each extreme group
    lowest_mean = low_exp_encoded_df.mean(axis=0)
    highest_mean = high_exp_encoded_df.mean(axis=0)

    # Generate offset using average gene expression in original dataset
    offset_latent_space = highest_mean - lowest_mean
    offset_latent_space_df = pd.Series.to_frame(offset_latent_space).T

    # output lowest and highest expressing samples
    low_exp_encoded_df.to_csv(lowest_file, sep='\t', float_format="%.5g")
    high_exp_encoded_df.to_csv(highest_file, sep='\t', float_format="%.5g")

    # ouput gene space offset vector
    offset_latent_space_df.to_csv(offset_file, sep='\t', float_format="%.5g")
    