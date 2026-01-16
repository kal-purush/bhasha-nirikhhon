# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 14:50:57 2016

@author: naamas
"""

import unittest
import santander
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
import numpy as np
class CreateData(unittest.TestCase):
      def setUp(self):
          submit=0  
          p='C:\\Temp\\train.csv'
          self.data=santander.Data(p,submit)
          self.X=self.data.read_file()
          self.train_size=100
          self.test_size=100
          self.data.divide_data(self.X,self.train_size,self.test_size)
          
class TestStringMethods(CreateData):
    
   
    def test_data_read_file(self):
        
        self.assertEqual(self.X.shape, (76020, 371))
        
    def test_data_divide_data(self):
      
        self.train_size=100
        self.test_size=100
        self.data.divide_data(self.X,self.train_size,self.test_size)
        self.assertEqual(self.data.X_train.shape, (self.train_size, 369))
        self.assertEqual(self.data.X_test.shape, (self.test_size, 369))
        self.assertEqual(self.data.y_train.shape, (self.train_size,))
        self.assertEqual(self.data.y_test.shape, (self.test_size,))
        
    def test_data_build_a_new_mat(self):
        
       
        ind=self.data.feature_selection(10,0)
        self.data.build_a_new_mat(ind)
      
         
        self.assertEqual(self.data.X_train.shape[1],len(ind) )
        self.assertEqual(self.data.X_test.shape[1], len(ind) )
     
    def test_change_values_of_feature(self):
        val=-999999
        new_val=2.71
        feature_name="var3"
        count=sum(self.data.X_train[feature_name].values==val)
        count_new=sum(self.data.X_train[feature_name].values==new_val)
        self.data.change_values_of_feature(val,new_val,feature_name)
        self.assertEqual(sum(self.data.X_train[feature_name].values==val),0 ) 
        self.assertEqual(sum(self.data.X_train[feature_name].values==new_val),count+count_new ) 
        
    def learn_model_test(self):
        check_flag=1
        probabilty_flag=1
        model=AdaBoostClassifier(n_estimators=400, learning_rate=0.03)
        learn=santander.Learn(model)
        [test_pred,train_pred]=learn.classify(self.data.X_train,self.data.y_train,self.data.X_test,self.data.y_test,probabilty_flag,check_flag)
        self.assertEqual(len(test_pred),self.test_size)
        self.assertEqual(len(train_pred),self.train_size)
        if(probabilty_flag):
            self.assertTrue((np.logical_and(train_pred>=0,train_pred<=1)).all)
            self.assertTrue((np.logical_and(test_pred>=0,test_pred<=1)).all)
            
def run_single_test(testname):
    suite = unittest.TestSuite()
    suite.addTest(TestStringMethods(testname))
    unittest.TextTestRunner(verbosity=2).run(suite)

def TheTestSuite():
    return unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)

def run_all_tests():
    unittest.TextTestRunner(verbosity=2).run(TheTestSuite())

if __name__ == '__main__':
#    run_all_tests()
    run_single_test('learn_model_test')