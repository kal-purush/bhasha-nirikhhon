#! /usr/env python
# -*- coding: utf8 -*-


import math, time
import numpy as np
import matplotlib.pyplot as plt
import multiprocessing as mp


class SVMClassifier:
    def __init__(self):
        self.source_path = "/home/marshao/SVMClassifier/"
        self.alphas = []  # list of langarange multipliers of Samples
        self.b = 0  # b for determine function
        self.TrainingSamples = []  # Matrix of Training Sample
        self.TrainingLabels = []  # Matrix of Training Labels
        self.CVSamples = []  # Matrix of CV samples
        self.CVLabels = []
        self.TestingSampels = []  # Matrix of test samples
        self.TestingLabels = []  # Marix of testing labels
        self.predicts = []  # Matrix of predicts
        self.T = 0.001  # Tolerance(Accuracy) of System
        self.C = 10.0  # Penotal Coefficients
        self.Step = 0.01  # Min step length of alpha1
        self.Max_iter = 3000  # Max loop time
        self.Models_Dict = []  # Dictonary to Store Models
        self.KernalType = 'g'  # predefine the Kernal Type is linear Kernal
        self.KernalCatch_Dict = {}  # Kernal Catch Values Dictionary
        self.KernalCatch_Matrix = np.zeros((1, 1))  # Kernal Catch Values Matrix
        self.GaussinSigma = 8.0  # Define a default value of Delta for Gaussin Kernal
        self.PolinomailR = 1.0  # Default value of Polinomail Kernal R
        self.Polinomaild = 2.0  # Default value of Polinomail Kernal d
        self.alpha1Idx = 0  # Index of Alpha 1
        self.alpha2Idx = 0  # Index of Alpha 2
        self.PredictResult = []  # List to save predictions
        self.ModelLearningCurve = []  # list to catch Eidx of Training Set for each model
        self.Eidx_Training_list = []  # A list to catch Eidx values of Train samples
        self.Lidx_Training_list = []
        self.Eidx_Training = 0.0
        self.Lidx_Training = 0
        self.Eidx_CV = 0.0
        self.Lidx_CV = 0

    def LoadData(self, model, Training_source=None, Testing_source=None, CrossValidation_source=None):
        '''Load Samples Data into Numpy Matrix
           Initial Parameters
        '''
        if Training_source is None:
            Training_source = 'TrainingSamples2.csv'
        if Testing_source is None:
            Testing_source = 'TestingSamples.csv'
        if CrossValidation_source is None:
            CrossValidation_source = 'CVSamples.csv'

        # Reset all sample catches to 0
        self.TrainingSamples = []  # Matrix of Training Sample
        self.TrainingLabels = []  # Matrix of Training Labels
        self.CVSamples = []  # Matrix of CV samples
        self.CVLabels = []
        self.TestingSampels = []  # Matrix of test samples
        self.TestingLabels = []  # Marix of testing labels
        self.predicts = []
        self.Eidx_Training_list = []

        fn = open(Training_source, "r")
        for line in fn:
            xVariable = []
            line = line[:-1]  # Remove the /r/n
            vlist1 = line.split("/r")
            if vlist1[0] == "": continue  # Omit empty line
            vlist = vlist1[0].split(",")
            # Get xVariables from Training Set
            for item in vlist[:-1]:
                xVariable.append(float(item))
            self.TrainingSamples.append(xVariable)

            # Get Lables from Training Set
            if float(vlist[-1]) == 0:
                label = -1
            else:
                label = float(vlist[-1])
            self.TrainingLabels.append(label)
        print "Loaded %s Training Samples" % len(self.TrainingSamples)
        fn.close()

        if model == 'Testing':
            fn = open(Testing_source, 'r')
            for line in fn:
                xVariable = []
                line = line[:-1]  # Remove the /r/n
                vlist1 = line.split("/r")
                if vlist1[0] == "": continue
                vlist = vlist1[0].split(",")
                # Get xVariables from Testing Set
                for item in vlist[:-1]:
                    xVariable.append(float(item))
                self.TestingSampels.append(xVariable)

                # Get Lables from Testing Set
                if float(vlist[-1]) == 0:
                    label = -1.0
                else:
                    label = float(vlist[-1])
                self.TestingLabels.append(label)
            print "Loaded %s Testing Samples" % len(self.TestingSampels)
            fn.close()
        elif model == 'CV':
            fn = open(CrossValidation_source, 'r')
            for line in fn:
                xVariable = []
                line = line[:-1]  # Remove the /r/n
                vlist1 = line.split("/r")
                if vlist1[0] == "": continue
                vlist = vlist1[0].split(",")
                # Get xVariables from CV Set
                for item in vlist[:-1]:
                    xVariable.append(float(item))
                self.CVSamples.append(xVariable)

                # Get Lables from CV Set
                if float(vlist[-1]) == 0:
                    label = -1.0
                else:
                    label = float(vlist[-1])
                self.CVLabels.append(label)
            print "Loaded %s CV Samples" % len(self.TestingSampels)
            fn.close()
        else:
            pass
        return

    def _Find_Alpha1(self):
        '''Find Alpha1 by checking the violation of KKT conditions'''
        pass

    def _Update_Variables(self, idx1=None, alpha1=None, idx2=None, alpha2=None, alpha_ini=False, alpha_val=None, b=None,
                          T=None,
                          C=None, Step=None, Max_iter=None, Sigma=None, Kernal_ini=False, KernalType=None,
                          PolinomialR=None, Polinomiald=None):
        '''Update Variables
            idx1: Alpha1
            idx2: Alpha2
            b
            T
            C
            Step
            Max_iter
        '''
        if idx1 != None and alpha1 != None:
            self.alphas[idx1] = alpha1 * 1.0
        if idx2 != None and alpha2 != None:
            self.alphas[idx2] = alpha2 * 1.0
        if b != None:
            self.b = b * 1.0
        if T != None:
            self.T = T * 1.0
        if C != None:
            self.C = C * 1.0
        if Step != None:
            self.Step = Step * 1.0
        if Max_iter != None:
            self.Max_iter = Max_iter

        if Sigma != None:
            self.GaussinSigma = Sigma * 1.0
        else:
            Sigma = self.GaussinSigma * 1.0

        if PolinomialR != None:
            self.PolinomailR = PolinomialR * 1.0
        else:
            PolinomialR = self.PolinomailR * 1.0

        if Polinomiald != None:
            self.Polinomaild = Polinomiald * 1.0
        else:
            Polinomiald = self.Polinomaild * 1.0

        if KernalType != None:
            self.KernalType = KernalType
        else:
            KernalType = self.KernalType

        # Initalize self.Kernal_Catch
        if Kernal_ini:
            print "System is initializing Kernal Catch:"
            stars = 0
            SampleCount = len(self.TrainingSamples)
            self.KernalCatch_Matrix = np.ones(shape=(SampleCount, SampleCount))
            for idx1 in range(SampleCount):
                # Formatting Output
                stars += 1
                if stars < 78:
                    print "*",
                else:
                    print ''
                    stars = 0
                for idx2 in range(SampleCount):
                    # Initailize the Kernal_Catch into Dictionary
                    # Initialize the Kernal values into Numpy Matrix
                    x1 = self.TrainingSamples[idx1]
                    x2 = self.TrainingSamples[idx2]
                    # if self.KernalCatch_Dict.has_key(key1):
                    #    continue
                    if KernalType == 'l':
                        KernalVal = self._Cal_Linear_Kernal(x1, x2)
                    elif KernalType == 'g':
                        KernalVal = self._Cal_Gaussin_Kernal(x1, x2, Sigma)
                    else:
                        KernalVal = self._Cal_Polinomial_Kernal(x1, x2, PolinomialR, Polinomiald)
                    self.KernalCatch_Matrix.itemset((idx1, idx2), KernalVal)

            print ''

        # Initialize or update Alpha list
        if alpha_ini:
            SampleCount = len(self.TrainingLabels)
            if alpha_val is None:
                alpha_val = float(0.1)
                self.alphas = [alpha_val] * SampleCount
            else:
                # i = 0
                if len(self.alphas) == 0:
                    # This situation match two status:
                    # 1. Initiate self.alphas with a alpha value Ex(0.1)
                    # 2. Load Alphas from Model File
                    if isinstance(alpha_val, list):
                        self.alphas = alpha_val
                    else:
                        self.alphas = [alpha_val] * SampleCount
                else:
                    # There is also two situtions:
                    # 1. alpha_val is a float, then this is alpha initializtion
                    # 2. alpha_val is a list, them this is load alpha from modle file
                    self.alphas = alpha_val
                    if isinstance(alpha_val, list):
                        if len(self.alphas) == len(alpha_val):
                            self.alphas = alpha_val
                        else:
                            print "Model does not match with Training Samples."
                    else:
                        self.alphas = [alpha_val] * SampleCount

    def _Find_Alpha2(self, idx1, E1, KernalType=None):
        '''Find Alpha2 when Alpha1 is decided
        Step1: Find Alpha2 by Max|E1-E2|
        Step2: If (Alpha2New-Alpha2old)<Step, then find Alpha2 from boundary samples
        Step3: If (Alpha2New-Alpha2old)<Step, then find Alpha2 from all samples
        '''
        if KernalType is None:
            KernalType = self.KernalType
        found, alpha2, eta = self._Find_Alpha2_Nonbound(idx1, E1, KernalType)
        if found == False:
            found, alpha2, eta = self._Find_Alpha2_MaxE(idx1, E1, KernalType)
        return found, alpha2, eta

    def _Find_Alpha2_MaxE(self, idx1, E1, KernalType=None):
        '''Find Alpha2 when Alpha1 is decided
        Step1: Find Alpha2 by Max|E1-E2|
        Step2: If (Alpha2New-Alpha2old)<Step, then find Alpha2 from boundary samples
        Step3: If (Alpha2New-Alpha2old)<Step, then find Alpha2 from all samples
        '''
        if KernalType is None:
            KernalType = self.KernalType

        alpha1 = idx1
        alpha2 = 0
        # gap is |E1 - E2|
        gap = 0
        eta = 0
        found = False
        # print "   Find alpha 2 from all"
        for idx2 in range(len(self.alphas)):
            if alpha1 == idx2: continue
            # eta is the distance of two Alphas, it must be > 0. If eta < 0, then the eta must be wrong
            eta_tmp = self._Cal_eta(idx1, idx2, KernalType)
            if eta_tmp < 0:
                print "   eta %s should not be less than 0." % eta_tmp
                continue
            E2 = self._Cal_Ei(idx2, KernalType)
            tmp = math.fabs(E1 - E2)
            if tmp > gap:
                gap = tmp
                alpha2 = idx2
                eta = eta_tmp
                found = True
        # if found:print "   @@@-2 For Alpha1 at %s the best All Alpha2 is %s"%(idx1, alpha2)
        return found, alpha2, eta

    def _Find_Alpha2_Nonbound(self, idx1, E1, KernalType=None):
        '''Find the alpha2 with Max |E1-E2| and which value is not the boudary value, boundary value is [0-C]'''
        if KernalType is None:
            KernalType = self.KernalType

        alpha1 = idx1
        alpha2 = 0
        # gap is |E1 - E2|
        gap = 0
        eta = 0
        found = False
        # print "   Find alpha 2 from non-boundary"
        for idx2 in range(len(self.alphas)):
            # Two alpha should not be same
            if alpha1 == idx2: continue
            # Alpha2 should not have the boundary value
            if (self.alphas[idx2] != 0 and self.alphas[idx2] != self.C):
                # eta is the distance of two Alphas, it must be > 0. If eta < 0, then the eta must be wrong
                eta_tmp = self._Cal_eta(idx1, idx2, KernalType)
                if eta_tmp < 0:
                    print "   eta %s should not be less than 0." % eta_tmp
                    continue
                E2 = self._Cal_Ei(idx2, KernalType)
                tmp = math.fabs(E1 - E2)
                if tmp > gap:
                    gap = tmp
                    alpha2 = idx2
                    eta = eta_tmp
                    found = True
        # if found:print "   @@@-2 For Alpha1 at %s the best Non-Boundary Alpha2 is %s"%(idx1, alpha2)
        return found, alpha2, eta

    def _Cal_F(self, x2, KernalType=None, Mode=None):
        '''The determine function F(x) to calculate the prediciton values
            x2 is the sample (vector of atributes Ex:[0.34, 0.21, 0.57]) waiting to be predicted
            Mode to distiguish Tr:Traing or Te:Testing
        '''
        # Setting the default Kernal type
        if KernalType is None:
            KernalType = self.KernalType

        if Mode is None:
            Mode = 'Tr'  # Training Mode

        val = []
        # Calculate Kernal, return value is a ndarray
        if Mode == 'Tr':
            # for x1 in range(len(self.TrainingSamples)):
            #    val.append(self._Fetch_Kernal(x1, x2))
            Kernal_Val = self._Fetch_Kernal(x2)
        elif Mode == 'Te':
            for x1 in self.TrainingSamples:
                val.append(self._Cal_Kernal(x1, x2, KernalType))
            Kernal_Val = np.array(val)
        else:
            print "Wrong Mode in Prediction"
            return

        # Kernal_Val = np.array(val)
        # Vectorize label y and alpha into ndarry for the next step of calculation
        y = np.array(self.TrainingLabels)
        alpha = np.array(self.alphas)
        # print "K, Y, A, %s %s"%( len(y), len(alpha))

        prediction_value = sum(alpha * y * Kernal_Val) + self.b
        # print prediction_value
        # print "Prediction Lable of data sample %s is %s"%(x2, prediction_value)
        return round(prediction_value, 5)

    def _Fetch_Kernal(self, idx1, idx2=None):
        '''Fetch Kernal value from calculated Kernal Catch
           idx1 and idx2 are the indexs
        '''
        # key = str(idx1)+'-'+str(idx2)
        # return  self.KernalCatch_Dict[key]
        if idx2 is None:
            return self.KernalCatch_Matrix[idx1, :]
        else:
            return self.KernalCatch_Matrix[idx1, idx2]

    def _Cal_Kernal(self, x1, x2, mode=None, sigma=None, R=None, d=None):
        '''Kernal function to calculate Kernals
            Linear Kernal
            Gaussin Kernal RBF
            Polinominal Kernal
            Kernals are based on the Dot.Product of X-Predicting and X-Samples
            x1 and x2 are the sample value
            x1 and x2 should be type of python list, not vector.
        '''
        if mode is None:
            mode = self.KernalType
        if sigma is None:
            sigma = self.GaussinSigma
        if R is None:
            R = self.PolinomailR
        if d is None:
            d = self.Polinomaild

        if mode == 'l':
            return self._Cal_Linear_Kernal(x1, x2)
        elif mode == 'g':
            return self._Cal_Gaussin_Kernal(x1, x2, sigma)
        elif mode == 'p':
            return self._Cal_Polinomial_Kernal(x1, x2, R, d)
        else:
            print "No such kernal type, kernal must be in l/g/p"

    def _Cal_Linear_Kernal(self, x1, x2):
        '''Calculation of linear Kernal
            Vector Solution: x1 and x2 are the sample vectors
        '''
        val = np.dot(x1, x2)
        # print "Linear Kernal Value is %s"%val
        return round(val, 5)

    def _Cal_Gaussin_Kernal(self, x1, x2, sigma):
        '''Calculation of Gaussion Kernal
            Vector Solution: x1 and x2 are the sample vectors
            Sigma is squared diviation with default value 8
        '''
        # Convert x1 and x2 into Numpy ndarray
        x1 = np.array(x1)
        x2 = np.array(x2)
        val = math.exp((-1) * (np.sum(np.square(x1 - x2)) / (2 * sigma * sigma)))
        # val = math.exp((-1)*(math.pow(np.linalg.norm((x1-x2)),2)/(2*sigma*sigma)))
        # print val
        return round(val, 5)

    def _Cal_Polinomial_Kernal(self, x1, x2, R, d):
        '''Calculation of Polinomial Kernal
        Vector Solution: x1 and x2 are the sample vectors
        R is the constant with default value 1
        d is the power degree with default value 2
        '''
        val = math.pow((self._Cal_Linear_Kernal(x1, x2) + R), d)
        return round(val, 5)

    def _Cal_Ei(self, idx, KernalType=None):
        '''Function to calculate difference between label and prediction
        E_idx=f(x_idx)-y_idx
        idx is the index of sample vector waiting for be predicted
        Kernal_Type is the type of Kernal in Using
        Will this E1 function only usful when Training, otherwise the idx may be ambigous to pointing to sample in Training set or Test Set.Lets make is only for Tranining set first
        '''
        # Define the Kernal Type
        if KernalType is None:
            KernalType = self.KernalType

        # x2 is the sample vector waiting for be predicted.
        predict = self._Cal_F(idx, KernalType)
        Eidx = predict - self.TrainingLabels[idx]

        # Update Error catch of this training
        self.Eidx_Training_list[idx] = abs(Eidx)
        return round(Eidx, 5)

    def _Cal_Alpha1(self, idx1, idx2, new_alpha2):
        '''Function to renew Alpha1
            idx1 and idx 2 are the old alpha1 and alpha2 index
            new_alpha2 is the new alpha2 value
        '''
        y1 = self.TrainingLabels[idx1]
        y2 = self.TrainingLabels[idx2]
        old_alpha1 = self.alphas[idx1]
        old_alpha2 = self.alphas[idx2]

        new_alpha1 = old_alpha1 + y1 * y2 * (old_alpha2 - new_alpha2)
        # print "New Alpha1 %s value is %s"%(idx1, new_alpha1)
        return round(new_alpha1, 5)

    def _Cal_Alpha2(self, idx1, idx2, E1, eta, C=None, Step=None, KernalType=None):
        '''Function to renew Alpha2
            idx1 is the index of old Alpha1
            idx2 is the index of old Alpha2
            E1 is the Ei value for alpha with index idx1
            eta is the eta value for Alpha1 and Alpha2
            L and H are the low and high border of new Alpha2 value
            C is the penalty coefficient
            Step is min change of Alpha2
        '''
        if KernalType is None:
            KernalType = self.KernalType
        if C is None:
            C = self.C
        if Step is None:
            Step = self.Step

        y1 = self.TrainingLabels[idx1]
        y2 = self.TrainingLabels[idx2]
        # Get the value of Alpha
        old_alpha1 = self.alphas[idx1]
        old_alpha2 = self.alphas[idx2]
        L = 0.0
        H = 0.0
        valid = True

        # Calculate new alpha2
        E2 = self._Cal_Ei(idx2, KernalType)
        # print "   E1-E2=%s,  eta = %s, y2*(E1-E2)=%s, y2*(E1-E2)/eta=%s"%((E1-E2), eta, (y2*(E1-E2)),(y2*(E1-E2)/eta))
        new_alpha2 = old_alpha2 + y2 * (E1 - E2) / eta

        # Set boundary for new_alpha2_value value
        if y1 == y2:
            L = max(0.0, (old_alpha2 + old_alpha1 - C))
            H = min(C, (old_alpha2 + old_alpha1))
        else:
            L = max(0.0, (old_alpha2 - old_alpha1))
            H = min(C, (C + old_alpha1 + old_alpha2))
        if L == H:
            valid = False
            return valid, old_alpha2
        # Clip new_alpha2_value value
        if new_alpha2 > H:
            clipped_new_alpha2 = H
        elif new_alpha2 < L:
            clipped_new_alpha2 = L
        else:
            clipped_new_alpha2 = new_alpha2

        # print "   For Alpha2 at index %s, old value is %s, new value is %s, clipped value is %s, L is %s, H is %s"%(idx2, old_alpha2, new_alpha2, clipped_new_alpha2, L, H)
        # Vrifile whether the clipped_new_alpha2 moved big enough > step
        if abs(clipped_new_alpha2 - old_alpha2) < Step:
            # ---print "   Old_Alpha2:Value %s:%s - clipped_New_Alpha2:%s = Diff %s cannot provide enough change to old Alpha2"%(idx2, old_alpha2, clipped_new_alpha2, abs(clipped_new_alpha2 - old_alpha2))
            valid = False

        return valid, round(clipped_new_alpha2, 5)

    def _Cal_eta(self, idx1, idx2, KernalType=None):
        '''Function to calcate eta
        eta = K11+K22-2K12
        K is Knernal,
        idx1 and idx2 is the index of Training Samples, also correspond to the index of Alpha
        Kernal Type is the Kernal Type
        Presume this eta Calculaton will only be using in Training Set
        '''
        # Define the default Kernal Type
        if KernalType is None:
            KernalType = self.KernalType

        K11 = self._Fetch_Kernal(idx1, idx1)
        K22 = self._Fetch_Kernal(idx2, idx2)
        K12 = self._Fetch_Kernal(idx1, idx2)
        eta = K11 + K22 - 2 * K12
        # print "eta value for x1 %s and x2 %s is %s"%(idx1, idx2, eta)
        return round(eta, 5)

    def _Cal_b(self, idx1, idx2, new_alpha1, new_alpha2, KernalType=None, C=None):
        '''Function to calculate b value when Alpha1 and Alpha2 are renewed'''
        if KernalType is None:
            KernalType = self.KernalType
        if C is None:
            C = self.C

        old_b = self.b
        old_alpha1 = self.alphas[idx1]
        old_alpha2 = self.alphas[idx2]
        y1 = self.TrainingLabels[idx1]
        y2 = self.TrainingLabels[idx2]
        K11 = self._Fetch_Kernal(idx1, idx1)
        K12 = self._Fetch_Kernal(idx1, idx2)
        K22 = self._Fetch_Kernal(idx2, idx2)
        K21 = self._Fetch_Kernal(idx2, idx1)
        E1 = self._Cal_Ei(idx1, KernalType)
        E2 = self._Cal_Ei(idx2, KernalType)

        # Calculate b
        new_b1 = (old_alpha1 - new_alpha1) * y1 * K11 + (old_alpha2 - new_alpha2) * y2 * K21 - E1 + old_b
        new_b2 = (old_alpha1 - new_alpha1) * y1 * K12 + (old_alpha2 - new_alpha2) * y2 * K22 - E2 + old_b
        if new_alpha1 > 0 and new_alpha1 < C:
            new_b = new_alpha1
        elif new_alpha2 > 0 and new_alpha2 < C:
            new_b = new_alpha2
        else:
            new_b = (new_b1 + new_b2) / 2
        # print "   new b is %s"%new_b
        return round(new_b, 5)

    def _Cal_Sum_Errors(self, source=None, Diff_mode=None, KernalType=None, Model=None):
        '''
        This function is to sum of errors between Predict and YLabel
        :param source: 'CV' or 'Train' or 'Test'
        :param Diff_mode: 'Lidx'(Difference between Predict Label and Real Label), 'Eidx'(Difference between Predict Value and Real Label)
        :param Model: 'Tr' or 'Te', transfer to self._Cal_F to indicate whehter use index to fetch kernal value or use sample vector to calculate kernal value
        :return:
        '''
        if source is None:
            source = 'Train'
        if Diff_mode is None:
            Diff_mode = 'Lidx'

        if source == 'Train':
            samples = self.TrainingSamples
            labels = self.TrainingLabels
        elif source == 'CV':
            samples = self.CVSamples
            labels = self.CVLabels
        elif source == 'Test':
            samples = self.TestingSampels
            labels = self.TestingLabels
        else:
            print 'No such model'
            return

        if Diff_mode == 'Lidx' and source == 'Train':
            self.Lidx_Training_list = [0] * len(self.TrainingLabels)
            for i in range(len(samples)):
                if self._Cal_F(i, KernalType) > 0:
                    pre_label = 1
                else:
                    pre_label = -1
                self.Lidx_Training_list[i] = abs(pre_label - labels[i])
            self.Lidx_Training = sum(self.Lidx_Training_list)
        elif Diff_mode == 'Lidx' and source == 'CV':
            self.Lidx_CV_list = [0] * len(self.CVLabels)
            for i in range(len(samples)):
                if self._Cal_F(samples[i], KernalType, Mode='Te') > 0:
                    pre_label = 1
                else:
                    pre_label = -1
                self.Lidx_CV_list[i] = abs(pre_label - labels[i])
            self.Lidx_CV = sum(self.Lidx_CV_list)

    def Train_Model(self, C=None, T=None, Loop=None, KernalType=None, Step=None, Sigma=None, Model_File=None,
                    Max_iter=None):
        '''Function to train SVM models with training sample set
            C is penalty coefficient with Default value self.C = 10
            T is Tolerance coefficient with Default value self.T=0.001
            Max_iter is max times of iteration with Default value self.Max_iter = 3000
            Step is the minimal change of alpha with default value self.Step = 0.001
            Loop is the max times that the system will tolorance for no alpha be udpated
        '''
        # Setting Default Parameters
        if C is None:
            C = self.C
        if T is None:
            T = self.T
        if Loop is None:
            # loop is the times of no alphas be udpated
            Loop = 3
        if KernalType is None:
            KernalType = self.KernalType
        if Step is None:
            Step = self.Step
        if Sigma is None:
            sigma = self.GaussinSigma
        if Model_File is None:
            Model_File = 'SVMModel.csv'
        if Max_iter is None:
            Max_iter = self.Max_iter
        # Initiate the index of alpha1_idx and alpha2_idx as 0
        alpha1_idx = 0
        alpha2_idx = 0
        old_alpha1 = self.alphas[alpha1_idx]
        old_alpha2 = self.alphas[alpha2_idx]

        # Set Eidx for this training to 0
        self.Eidx_Training_list = [0.0] * len(self.TrainingLabels)

        # Max_iter the list of Alpha until the reach the max loop number or all alphas have no furthur change
        iter = 0
        passes = 0
        stars = 0
        while passes < Loop:
            updated_alpha = 0  # Alpha be changed in each loop
            iter += 1
            # Formatting the output
            print "Iteration %s is start" % iter
            print"--------------------------------------------------------------------"

            # find alpha1_idx's index by checking the violation of KKT condition
            for alpha1_idx in range(len(self.alphas)):
                y1 = self.TrainingLabels[alpha1_idx]
                E1 = self._Cal_Ei(alpha1_idx, KernalType)
                # print "E1 value is %s"%E1
                # ---print "@@@-1 For Alpha1 at %s with value %s, label y1 is %s, E1 is %s, C is %s, T is %s, alpha1 value is %s, y1*E1 = %s"%(alpha1_idx, old_alpha1, y1, E1, C, T, old_alpha1, y1*E1)
                # Formatting output
                stars += 1
                if stars < 78:
                    print "*",
                else:
                    print ''
                    stars = 0

                # KKT condition vilation checking
                if (old_alpha1 < C and (y1 * E1) < -T) or (old_alpha1 > 0 and (y1 * E1) > T):
                    done = False
                    # Find the most proper alpha2_idx's index
                    found, alpha2_idx, eta = self._Find_Alpha2(alpha1_idx, E1, KernalType)
                    # print "   Found Alpha2 is %s and Alpha2 is %s"%(found, alpha2_idx)
                    if found == False:
                        # Cannot find proper Alpha2 for Alpha1, change to an new alpha1_idx
                        # print "   Cannot find proper Alpha2 for Alpha1 %s, change to an new Alpha1" % alpha1_idx
                        continue

                    # Calculate new alpha2_idx
                    valid, clipped_new_alpha2 = self._Cal_Alpha2(alpha1_idx, alpha2_idx, E1, eta, C, Step, KernalType)
                    if valid == False:
                        # print "There is no valid value for alpha2 at index %s"%alpha2_idx
                        continue
                    else:
                        updated_alpha += 1

                    # Calculate new alpha1_idx
                    new_alpha1 = self._Cal_Alpha1(alpha1_idx, alpha2_idx, clipped_new_alpha2)

                    # Calculate b when new alpha1 and alpha2 be calculated out
                    new_b = self._Cal_b(alpha1_idx, alpha2_idx, new_alpha1, clipped_new_alpha2, KernalType, C)

                    # update variables
                    self._Update_Variables(idx1=alpha1_idx, alpha1=new_alpha1,
                                           idx2=alpha2_idx, alpha2=clipped_new_alpha2,
                                           b=new_b)
                    # ---print "@@@-3  Max_iter:%s, Alpha1:idx1 = %s:%s; Alpha2:idx2 = %s:%s; Updated to:  Alpha1_New:%s and Alpha2_New:%s and b_new:%s"%(iter,alpha1_idx, old_alpha1, alpha2_idx, old_alpha2, new_alpha1, clipped_new_alpha2,new_b)
                else:
                    # ---print "@@@-3 Alpha1:%s does not violate KKT conditions"%old_alpha1
                    pass
            # Count the undated Alpha for each iteration
            print ''
            if iter > Max_iter: break
            if updated_alpha == 0:
                passes += 1
                j = 0
                for alpha in self.alphas:
                    if alpha != 0.1:
                        j += 1
                print "In iteration %s, no more alphas be updated, in total %s alphas be updated" % (iter, j)
                # print "Passes = %s"%passes
            else:
                passes = 0
            print "In Iteraton %s, %s alphas has been updated" % (iter, updated_alpha)
            print self.alphas
            print "____________________________________________________"
            print ""
        # Calculate Diff between predict value and y-Labels
        self.Eidx_Training = sum(self.Eidx_Training_list)
        # Calculate Diff between predict labels and y-labels
        self._Cal_Sum_Errors(source='Train', Diff_mode='Lidx', KernalType=KernalType)
        self.Write_Model(Model_File)

    def Write_Model(self, destination=None):
        '''Write out model configurations to a file'''
        if destination is None:
            destination = 'SVMModel.csv'
        fn = open(destination, "w")
        # fn.write('C value is:'+str( self.C)+'\n')
        fn.write(str(self.C) + '\n')
        # fn.write('Gaussin Sigma value is:'+str(self.GaussinSigma)+'\n')
        fn.write(str(self.GaussinSigma) + '\n')
        # fn.write('Alpha values are: \n')
        for item in self.alphas:
            fn.write(str(item) + '\n')

        print 'Model has been writen to %s' % destination
        fn.close()

    def Load_Model(self, Model_File=None):
        '''Load Model Parameters from SVMModel.CSV'''
        if Model_File is None:
            Model_File = 'SVMModel.csv'
        fn = open(Model_File, 'r')
        C = float(fn.readline())
        GaussinSigma = float(fn.readline())
        alpha_val = []
        for line in fn:
            alpha_val.append(float(line))
        self._Update_Variables(C=C, Sigma=GaussinSigma, alpha_val=alpha_val, alpha_ini=True)

    def Cross_Validate_Model(self, KernalType=None, Output=None, From=None, Result=None):
        '''Function to validate models with CV sample set
            Output: The output desination file
            From: The source type of prediction performance analysis
            Result: The source of predoction performance analysis
        '''
        if KernalType is None:
            KernalType = self.KernalType
        if Output is None:
            Output = 'SVMCVTest.csv'
        if From is None:
            From = 'l'
        if Result is None:
            Result = self.PredictResult

        predict_label = []
        stars = 0
        self.Eidx_CV = 0.0
        print "Model Cross Validation Process is started:"
        print "--------------------------------------------------------"
        for x in range(len(self.CVSamples)):
            stars += 1
            if stars < 78:
                print "*",
            else:
                print ''
                stars = 0
            predict = self._Cal_F(self.CVSamples[x], KernalType, Mode='Te')
            # print 'predict on CV sample is %s'%predict

            if predict > 0:
                val = 1
            else:
                val = -1
            predict_label.append(val)
            self.Eidx_CV += abs(predict - self.CVLabels[x])
        print ''
        print "Model Prediction is completed"
        # Calculate the sum Diff between predict labels and Y-Labels
        self._Cal_Sum_Errors(source='CV', Diff_mode='Lidx', KernalType=KernalType)

        self.PredictResult = predict_label
        Result = self.PredictResult
        Precision, Recall, Accuracy = self.Performance_Diag(From, Result, Model='C')
        self.Write_Test(predict_label, Precision, Recall, destination=Output, Model='C')

    def Test_Model(self, KernalType=None, Output=None, From=None, Result=None):
        '''Function to Test models with test sample set
            Output: The output desination file
            From: The source type of prediction performance analysis
            Result: The source of predoction performance analysis
        '''
        if KernalType is None:
            KernalType = self.KernalType
        if Output is None:
            Output = 'SVMTest.csv'
        if From is None:
            From = 'l'
        if Result is None:
            Result = self.PredictResult

        predict_label = []
        stars = 0
        print "Model Prediction is started:"
        print "--------------------------------------------------------"
        for x in self.TestingSampels:
            stars += 1
            if stars < 78:
                print "*",
            else:
                print ''
                stars = 0
            if self._Cal_F(x, KernalType, Mode='Te') > 0:
                val = 1
            else:
                val = -1
            predict_label.append(val)
        print ''
        print "Model Prediction is completed"
        self.PredictResult = predict_label
        Precision, Recall, Accuracy = self.Performance_Diag(From, Result, Model='T')
        self.Write_Test(predict_label, Precision, Recall, destination=Output, Model='T')

    def Write_Test(self, predict_label, Precision=0.0, Recall=0.0, destination=None, Model='T'):
        '''Write out model configurations to a file'''
        if Model == 'T':
            y_labels = self.TestingLabels
        else:
            y_labels = self.CVLabels
        if destination is None:
            destination = 'SVMTest.csv'
        fn = open(destination, "w")
        fn.write("Prediciton_Label,Test_Label\n")
        for i in range(len(predict_label)):
            fn.write(str(predict_label[i]) + ',' + str(y_labels[i]) + "\n")
        fn.write("Precision,%s \n" % Precision)
        fn.write("Recall,%s \n" % Recall)
        print 'Test Result has been writen to %s' % destination
        fn.close()

    def Predict(self):
        '''Function to use model to predict values'''
        pass

    def Performance_Diag(self, From=None, Result=None, Model=None):
        '''Function to evaluate performance of models
        From: The source of the prediction(List or File)
        Result: The File of prediction result.
        Model is to differentiate from Testing and CrossValidateion
        Model value is in ['T','C']
        * Precision = True_Positive/(True_Pos + False_Pos)
        * Recall = True_Pos/(True_Pos _ False_Neg)
        '''
        TP = 0.0
        FP = 0.0
        FN = 0.0
        TN = 0.0

        if Model is None:
            Model = 'T'
        if Model == 'T':
            labels = self.TestingLabels
        elif Model == 'C':
            labels = self.CVLabels
        else:
            print 'No Such Model in Performance Diag'
            return

        if From is None:
            From = 'l'  # Predict results from list

        if From is 'l':
            if Result is None:
                Result = self.PredictResult
            for i in range(len(Result)):
                if Result[i] == 1:
                    if labels[i] == 1:
                        TP += 1
                    else:
                        FP += 1
                else:
                    if labels[i] == -1:
                        TN += 1
                    else:
                        FN += 1
        elif From is 'f':
            if Result is None:
                Result = 'SVMTest.csv'
            fn = open(Result, 'r')
            i = 0
            fn.readline()
            for line in fn:
                line = line[:-1]  # Remove the /r/n
                vlist1 = line.split("/r")
                if vlist1[0] == "": continue
                vlist = vlist1[0].split(",")
                if vlist[0] == 'Precision' or vlist[0] == 'Recall': continue
                if float(vlist[0]) == 1.0:
                    if self.TestingLabels[i] == 1:
                        TP += 1
                    else:
                        FP += 1
                else:
                    if self.TestingLabels[i] == -1:
                        TN += 1
                    else:
                        FN += 1
                i += 1
        else:
            print "No such option"

        if (TP + FP) == 0:
            Precision = 0
        else:
            Precision = round(TP / (TP + FP), 2)
        if (TP + FN) == 0:
            Recall = 0
        else:
            Recall = round(TP / (TP + FN), 2)
        Overall_Accuracy = round((TP + TN) / (TP + FP + TN + FN), 2)
        print "TP=%s, FP=%s, TN=%s, FN=%s, Precision Rate is %s and Recall Rate is %s, Overall Accurate is %s" % (
            TP, FP, TN, FN, Precision, Recall, Overall_Accuracy)
        return Precision, Recall, Overall_Accuracy

    def Plot_Learning_Curve(self, model=None, base_parameter=None):
        '''
        Plot Learning Curves
        :return:
        '''
        # The least parameter count
        if model is None:
            model = 'Parameter'
            base_parameter = 5
        yTrain = []
        yCV = []
        x = range(1, (len(self.ModelLearningCurve) + 1))
        for i in range(len(self.ModelLearningCurve)):
            yTrain.append(self.ModelLearningCurve[i][0])
            yCV.append(self.ModelLearningCurve[i][1])
        print x
        print yTrain
        print yCV

        plt.plot(x, yTrain, 'ob-', c='g', label=u'TrainingError')
        plt.plot(x, yCV, 'ob-', c='r', label=u'CVError')
        for i, j in zip(x, yTrain):
            plt.annotate(str(j), xy=(i, j))
        for i, j in zip(x, yCV):
            plt.annotate(str(j), xy=(i, j))
        plt.xlabel(u'Number of Parameters')
        plt.ylabel((u'Sum of Errors'))
        plt.show()

    def pause(self):
        programPause = raw_input("Press any key to continue")


def main():
    import profile
    import pstats

    # profile.run("run()", "prof1.txt")
    # p = pstats.Stats('prof1.txt')
    # p.sort_stats('time').print_stats()
    # singal_run()
    # batch_test_parameters()
    # batch_test_sample_sizes()
    batch_test_C_Sigma()


def singal_run():
    model = SVMClassifier()
    model.LoadData('CV', Training_source='TrainingLO.csv', CrossValidation_source='CVLO.csv')
    model._Update_Variables(C=1.0, Sigma=0.1, T=0.001, Step=0.01, KernalType='g', alpha_ini=True, alpha_val=0.1,
                            Kernal_ini=True, Max_iter=3000)
    model.Train_Model(Loop=3, Model_File='StockTrainingModel2.csv')
    model.Load_Model('StockTrainingModel2.csv')
    model.Cross_Validate_Model(KernalType='g', Output='StockTest2.csv')
    # model.Test_Model(KernalType='g', Output='StockTest2.csv')
    Precesion, Recall, Accuracy = model.Performance_Diag(Model='C')
    print(
        'C: %s,  Sigma: %s, Precesion: %s, Recall: %s, Accuracy: %s, Eidx_Training: %s, Eidx_CV: %s \n' % (
            1.0, 0.1, Precesion, Recall, Accuracy, model.Eidx_Training, model.Eidx_CV))


def batch_test_C_Sigma():
    model = SVMClassifier()
    model.LoadData('CV', Training_source='TrainingLO.csv', CrossValidation_source='CVLO.csv')
    # C = [0.6, 0.7, 0.8, 1.0, 1.3, 1.5, 1.8, 2.0, 3.0, 4.0]
    Sigma = [0.06, 0.08, 0.1, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0]
    C = [1.0]
    # Sigma = [0.1]
    fn = open('StockMultiModelResults.csv', "w+")
    for each_C in C:
        for each_Sigma in Sigma:
            # tmp = []
            model._Update_Variables(C=each_C, Sigma=each_Sigma, T=0.001, Step=0.01, KernalType='g', alpha_ini=True,
                                    alpha_val=0.1,
                                    Kernal_ini=True)
            model.Train_Model(Loop=3, Model_File='StockTrainingModel2.csv')
            model.Load_Model('StockTrainingModel2.csv')
            model.Cross_Validate_Model(KernalType='g', Output='StockTest2.csv')
            Precesion, Recall, Accuracy = model.Performance_Diag(Model='C')
            model.ModelLearningCurve.append([model.Eidx_Training, model.Eidx_CV])
            fn.writelines(
                'C: %s,  Sigma: %s, Precesion: %s, Recall: %s, Accuracy: %s, Eidx_Training: %s, Eidx_CV: %s \n' % (
                    each_C, each_Sigma, Precesion, Recall, Accuracy, model.Eidx_Training, model.Eidx_CV))
    fn.close()
    model.Plot_Learning_Curve()


def multi_batch_test_C_Sigma():
    model = SVMClassifier()
    model.LoadData('CV', Training_source='TrainingLO.csv', CrossValidation_source='CVLO.csv')
    # C = [0.6, 0.7, 0.8, 1.0, 1.3, 1.5, 1.8, 2.0, 3.0, 4.0]
    Sigma = [0.06, 0.08, 0.1, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0]
    C = [1.0]
    # Sigma = [0.1]
    processors = 7
    processes = []
    index_beg = 0

    # fn = open('StockMultiModelResults.csv', "w+")
    for each_C in C:
        for each_Sigma in Sigma:
            p = mp.Process(target=task, args=(each_C, each_Sigma,))
            processes.append(p)
            # model._Update_Variables(C=each_C, Sigma=each_Sigma, T=0.001, Step=0.01, KernalType='g', alpha_ini=True,
            #                         alpha_val=0.1,
            #                         Kernal_ini=True)
            # model.Train_Model(Loop=3, Model_File='StockTrainingModel2.csv')
            # model.Load_Model('StockTrainingModel2.csv')
            # model.Cross_Validate_Model(KernalType='g', Output='StockTest2.csv')
            # Precesion, Recall, Accuracy = model.Performance_Diag(Model='C')
            # model.ModelLearningCurve.append([model.Eidx_Training, model.Eidx_CV])
            # fn.writelines(
            #    'C: %s,  Sigma: %s, Precesion: %s, Recall: %s, Accuracy: %s, Eidx_Training: %s, Eidx_CV: %s \n' % (
            #        each_C, each_Sigma, Precesion, Recall, Accuracy, model.Eidx_Training, model.Eidx_CV))
    # fn.close()
    processes_pool(tasks=processes, processors=processors)
    model.Plot_Learning_Curve()


def task(each_C, each_Sigma):
    model = SVMClassifier()
    fn = open('StockMultiModelResults.csv', "w+")
    model._Update_Variables(C=each_C, Sigma=each_Sigma, T=0.001, Step=0.01, KernalType='g', alpha_ini=True,
                            alpha_val=0.1,
                            Kernal_ini=True)
    model.Train_Model(Loop=3, Model_File='StockTrainingModel2.csv')
    model.Load_Model('StockTrainingModel2.csv')
    model.Cross_Validate_Model(KernalType='g', Output='StockTest2.csv')
    Precesion, Recall, Accuracy = model.Performance_Diag(Model='C')
    model.ModelLearningCurve.append([model.Eidx_Training, model.Eidx_CV])
    fn.writelines(
        'C: %s,  Sigma: %s, Precesion: %s, Recall: %s, Accuracy: %s, Eidx_Training: %s, Eidx_CV: %s \n' % (
            each_C, each_Sigma, Precesion, Recall, Accuracy, model.Eidx_Training, model.Eidx_CV))
    fn.close()


def batch_test_parameters():
    model = SVMClassifier()
    # TrainingSource = ['StockTrainingParameter1.csv', 'StockTrainingParameter2.csv', 'StockTrainingParameter3.csv',
    #                  'StockTrainingParameter4.csv', 'StockTrainingParameter5.csv', 'StockTrainingParameter6.csv']
    # CVSource = ['StockCVParameter1.csv', 'StockCVParameter2.csv', 'StockCVParameter3.csv', 'StockCVParameter4.csv',
    #            'StockCVParameter5.csv', 'StockCVParameter6.csv']
    TrainingSource = ['TrainingHO.csv', 'TrainingLO.csv']
    CVSource = ['CVHO.csv', 'CVLO.csv']
    fn = open('StockMultiParameterResults.csv', "w+")

    for i in range(len(TrainingSource)):
        model.LoadData('CV', Training_source=TrainingSource[i], CrossValidation_source=CVSource[i])
        model._Update_Variables(C=4.0, Sigma=0.8, T=0.001, Step=0.01, KernalType='g', alpha_ini=True,
                                alpha_val=0.1,
                                Kernal_ini=True, Max_iter=3000)
        model.Train_Model(Loop=3, Model_File='StockMultiParameterModel1.csv')
        model.Load_Model('StockMultiParameterModel1.csv')
        model.Cross_Validate_Model(KernalType='g', Output='StockMultiParameterTest1.csv')
        Precesion, Recall, Accuracy = model.Performance_Diag(Model='C')
        model.ModelLearningCurve.append([round(model.Eidx_Training, 4), round(model.Eidx_CV, 4)])
        fn.writelines(
            'C: %s,  Sigma: %s, Precesion: %s, Recall: %s, Accuracy: %s, Eidx_Training: %s, Eidx_CV: %s \n' % (
                4.0, 0.8, Precesion, Recall, Accuracy, model.Eidx_Training, model.Eidx_CV))
    fn.close()
    model.Plot_Learning_Curve()


def batch_test_sample_sizes():
    model = SVMClassifier()
    TrainingSource = ['Training_100-30.csv', 'Training_150-30.csv', 'Training_200-30.csv', 'Training_250-30.csv',
                      'Training_298-30.csv']
    CVSource = ['CV_100-30.csv', 'CV_100-30.csv', 'CV_100-30.csv', 'CV_100-30.csv', 'CV_100-30.csv']
    fn = open('StockSampleSizesResults.csv', "w+")

    for i in range(len(TrainingSource)):
        model.LoadData('CV', Training_source=TrainingSource[i], CrossValidation_source=CVSource[i])
        model._Update_Variables(C=4.0, Sigma=0.8, T=0.001, Step=0.01, KernalType='g', alpha_ini=True,
                                alpha_val=0.1,
                                Kernal_ini=True, Max_iter=3000)
        model.Train_Model(Loop=3, Model_File='StockSampleSizesModel1.csv')
        model.Load_Model('StockSampleSizesModel1.csv')
        model.Cross_Validate_Model(KernalType='g', Output='StockSampleSizesTest1.csv')
        Precesion, Recall, Accuracy = model.Performance_Diag(Model='C')
        # model.ModelLearningCurve.append([round(model.Eidx_Training,4), round(model.Eidx_CV,4)])
        model.ModelLearningCurve.append([round(model.Lidx_Training, 4), round(model.Lidx_CV, 4)])
        fn.writelines(
            'C: %s,  Sigma: %s, Precesion: %s, Recall: %s, Accuracy: %s, Eidx_Training: %s, Eidx_CV: %s, Lidx_Training: %s, Lidx_CV: %s\n' % (
                4.0, 0.8, Precesion, Recall, Accuracy, model.Eidx_Training, model.Eidx_CV, model.Lidx_Training,
                model.Lidx_CV))
    fn.close()
    model.Plot_Learning_Curve()


def processes_pool(tasks, processors):
    # This is a self made Multiprocess pool
    task_total = len(tasks)
    loop_total = task_total / processors
    # print "task total is %s, loop_total is %s" % (task_total, loop_total)
    alive = True
    task_finished = 0
    task_alive = 0
    task_remain = task_total - task_finished
    count = 0
    i = 0
    while i <= loop_total:
        # print "This is the %s round" % i
        for j in range(processors):
            k = j + processors * i
            # print "executing task %s" % k
            if k == task_total:
                break
            tasks[k].start()
            j += 1

        for j in range(processors):
            k = j + processors * i
            if k == task_total:
                break
            tasks[k].join()
            j += 1

        while alive == True:
            n = 0
            alive = False
            for j in range(processors):
                k = j + processors * i
                if k == task_total:
                    # print "This is the %s round of loop"%i
                    break
                if tasks[k].is_alive():
                    alive = True
                time.sleep(1)
        i += 1


if __name__ == '__main__':
    main()