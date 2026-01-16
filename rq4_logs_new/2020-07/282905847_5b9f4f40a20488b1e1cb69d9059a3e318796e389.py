import numpy as np 
import pandas as pd 
from sklearn import model_selection


if __name__=="__main__":
    # read the data
    df = pd.read_csv('input/train.csv')
    # create kfold column
    df['kfold'] = -1
    # shuffle the data and reset index
    df.sample(frac=1).reset_index(drop=True)

    # Create the kfold object
    kfold = model_selection.StratifiedKFold(n_splits=5, shuffle=False, random_state=42)

    # Separate new training and validation data
    for fold, (train_idx, val_idx) in enumerate(kfold.split(X=df,y=df.target.values)):
        print(len(train_idx),len(val_idx))
        df.loc[val_idx,'kfold'] = fold
    
    # save new k-fold dataset
    df.to_csv('input/train_folds.csv',index=False)
    
    