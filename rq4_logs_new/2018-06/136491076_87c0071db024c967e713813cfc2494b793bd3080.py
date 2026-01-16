import pandas as pd
import numpy as np
import os
import sys

"""
Use this script as interface to construct input data feed, and retrieve result output
Basically this is a model wrapper
It facilitates input data feeding to model, and forces models to output result in unified structure

Using preprocessed data 
"""

class Stats:
    """
    A class for storing statistics of model
    """
    def __init__(self,real_labels,guess_labels):
        """
        Compute statistics from prediction and real situation

        Parameters:
        - real_labels: pandas Series, real labels
        - guess_labels: pandas Series, prediction labels
        """
        # Compute confusion matrix of the prediction
        # confuse_mat[i,j] stores the ratio that people of class i being classified as in class j
        self._confuse_mat = np.zeros((2,2))
        for i in range(2):
            for j in range(2):
                self._confuse_mat[i,j]=np.mean((real_labels==i)&(guess_labels==j))

        # Compute correct and error rate from confusion matrix
        self._correct_rate = np.trace(self._confuse_mat)
        self._error_rate = 1-self._correct_rate
        
    @property
    def confuse_matrix(self):
        return self._confuse_mat

    @property
    def error_rate(self):
        return self._error_rate
    
    @property
    def correct_rate(self):
        return self._correct_rate

class ModelBase:
    """
    Base class for model
    """

    def __init__(self):
        cv_data_dir = "../Data/CrossValidation2/"
        # Train pandas.core.frame.DataFrame list from 10 folds
        self.train_df_list = []
        # Dev (test) pandas.core.frame.DataFrame list from 10 folds
        self.dev_df_list = []
        # Results are stored as a list of Stats from each fold of validation
        self.results = []
        f = open("../Data/preprocessed_train.csv")
        self.train_df = pd.read_csv(f)
        f.close()
        f = open("../Data/preprocessed_test.csv")
        self.test_df = pd.read_csv(f)
        f.close()


        """
        Intialize anything needed by cross validation procedure
        While model specific data is not initialized until in setup method
        """
        self.nfold = 10
        # For each fold
        for i in range(1,self.nfold+1):
            # Construct path
            path = cv_data_dir+"fold_"+str(i).zfill(2)+"/"
            train_path = path+"train.csv"
            dev_path = path+"dev.csv"
            # Load train data into dataframe list
            f = open(train_path)
            df = pd.read_csv(f)
            f.close()
            self.train_df_list.append(df)
            # Load test data into dataframe list
            f = open(dev_path)
            df = pd.read_csv(f)
            f.close()
            self.dev_df_list.append(df)

    def setup(self):
        """
        This is where we setup our models: defining and initializing parameters, etc.
        """
        pass
    
    def train(self,features,labels):
        """
        Parameters:
        - features: pandas.core.frame.DataFrame, the informations about passengers, such as sex, age, class etc.
        - labels: pandas.core.series.Series, whether or not the passenger survived, 0 for died, 1 for survived.

        Jobs:
        - This is where we update our model's parameters
        """
        pass
    
    def predict(self,features):
        """
        Predict whether somebody survived in the disaster

        Parameters:
        - features: pandas.core.frame.DataFrame, the informations about passengers, such as sex, age, class etc.

        Returns:
        - labels: pandas.core.series.Series, whether or not the passenger survived, 0 for died, 1 for survived.
        """
        return pd.Series([0]*len(features))

    def separate_data(self,df):
        """
        Separate data into features and labels (if it has labels)

        Parameters:
        - df: pandas.core.frame.DataFrame where data resides

        Returns:
        - features: pandas.core.frame.DataFrame, the informations about passengers, such as sex, age, class etc.
        - labels: pandas.core.series.Series, whether or not the passenger survived, 0 for died, 1 for survived.
        """
        # Simply retrieve Survived column from DataFrame
        labels = df["Survived"]
        # Get all column names of df, but without Survived column
        newcols = df.columns.drop("Survived")
        # Get these columns
        features = df[newcols]
        return features,labels

    def run_cv(self):
        """
        run the entire cross validation procedure
        """
        # We first setup your model
        self.setup()
        # For each fold
        for i in range(self.nfold):
            # Retrieve data
            train_df = self.train_df_list[i]
            dev_df = self.dev_df_list[i]
            # Separate features and labels in train set
            features,labels = self.separate_data(train_df)
            # We will be training your model here
            # You should update your model in train method
            self.train(features,labels)

            # Separate features and labels in dev (test) set
            features,labels = self.separate_data(dev_df)
            # This is where we test your trained model
            # Make sure you return the predictions as pandas.core.series.Series
            guess_labels = self.predict(features)

            # Compute test result
            this_result = Stats(labels,guess_labels)
            # Store this result
            self.results.append(this_result)

        return self.results
    
    def run_main(self,output_filename="predict"):
        """
        Run on the entire preprocessed train set, and then predict on (preprocessed) test set
        Generate output in Data/predict.csv
        """
        self.setup()
        train_features,train_labels = self.separate_data(self.train_df)
        self.train(train_features,train_labels)
        guess_labels = self.predict(self.test_df)
        output_dict ={}
        output_dict["PassengerId"] = self.test_df["PassengerId"]
        output_dict["Survived"] = guess_labels
        output_df = pd.DataFrame(output_dict)
        output_df.to_csv("../Data/{0}.csv".format(output_filename),index=False)
        return guess_labels
        

def summary(results):
    """
    Compute statistics of model results and display them

    Parameters:
    - results: a list of Stats, generated from 10 folds of validation
    """
    correct_rates = [result.correct_rate for result in results]
    confuse_mats = [result.confuse_matrix for result in results]
    # Expectation of correct rates
    correct_mean = np.mean(correct_rates)
    # Standard deviation of correct rates
    # Smaller std indicates more robust model!
    correct_std = np.std(correct_rates)
    # Expectation of confusion matrix
    confuse_mean = np.mean(confuse_mats,axis=0)
    # Standard deviation of confusion matrix
    # Similarily, smaller std means more robust on that case
    # For example, if confuse_std[0,0] is small, it tells us that the model is robust on predicting class 0
    confuse_std = np.std(confuse_mats,axis=0)
    print("Expected Correct Rate: {0:.2f}%".format(correct_mean*100))
    print("1/Correct Rate Std. (Robustness): {0:.2f}".format(1/correct_std))
    print("Expected Confusion Matrix:")
    print("   \t[0]\t[1]")
    print("[0]\t{0:.2f}%\t{1:.2f}%".format(confuse_mean[0,0]*100,confuse_mean[0,1]*100))
    print("[1]\t{0:.2f}%\t{1:.2f}%".format(confuse_mean[1,0]*100,confuse_mean[1,1]*100))
    print("1/Confusion Matrix Std. (Robustness):")
    print("   \t[0]\t[1]")
    print("[0]\t{0:.2f}\t{1:.2f}".format(1/confuse_std[0,0],1/confuse_std[0,1]))
    print("[1]\t{0:.2f}\t{1:.2f}".format(1/confuse_std[1,0],1/confuse_std[1,1]))
    return correct_mean,correct_std,confuse_mean,confuse_std

class SimpleModel(ModelBase):
    """
    Simple model derived form ModelBase class
    Show how to write model class
    Basically you will need to:
    - override setup method, to setup your model's parameters and other things
    - override train method. This is where you write the model's core algorithm
    - override predict method. This is where you use your model to predict whether someone survived
    But you can also extend other methods regarding your model's needs

    In this simple model, we simply compute the ratios of survival for man and woman in train set
    And then use these ratios as probability to randomly guess man and woman's survival in test set
    
    This may be too naive a model and not utilizing all information from train set
    But it will be sufficient to show you how to write your own model based on ModelBase class
    """
    def setup(self):
        # Define and initialize your model's parameters here
        self.man_survive_prob = 0
        self.woman_survive_prob = 0

    def train(self,features,labels):
        # Specify how to train your model here
        # All data you need will be features and labels as given in parameters

        # Retrieve Sex column
        sex = features["Sex"]
        # Get man and woman's survival records
        man_labels = labels[sex=="male"]
        woman_labels = labels[sex=="female"]

        # Update parameters of your model here

        # We will simply compute the ratios of survival
        self.man_survive_prob = np.mean(man_labels)
        self.woman_survive_prob = np.mean(woman_labels)

    def predict(self,features):
        # Compute and return your prediction here from given features
        # All data you need is features
        # Also model parameters should be already updated from train method

        # Initialize labels
        n = len(features)
        labels = pd.Series([0]*n)

        # Separate male and female
        sex = features["Sex"]
        rands = np.random.rand(n)
        # For man
        idx = sex=="male"
        labels[idx]=(rands[idx]<self.man_survive_prob).astype(int)
        # For woman
        idx = sex=="female"
        labels[idx]=(rands[idx]<self.woman_survive_prob).astype(int)

        return labels

if __name__=="__main__":
    """
    Demo run
    """
    # Create model
    model = SimpleModel()
    # Run model
    cv_results = model.run_cv()
    # Display results
    summary(cv_results)
    main_results = model.run_main()
    
