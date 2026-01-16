import numpy as np
from scipy.stats import bernoulli

class LinPHEStruct:
    def __init__(self, featureDimension, lambda_, c, a, p):
        self.d = featureDimension
        self.A = lambda_ * np.identity(n=self.d)
        self.lambda_ = lambda_

        self.b = np.zeros(self.d)

        self.a = int(a) ## hyperparam
        self.p = p
        self.c = c ## hyperparameter


        self.UserArmTrials= {}

        self.AInv = np.linalg.inv(self.A)
        self.UserTheta = np.zeros(self.d)
        self.time = 0

    def updateParameters(self, articlePicked_FeatureVector, click):
        self.A += np.outer(articlePicked_FeatureVector, articlePicked_FeatureVector)
        self.b += articlePicked_FeatureVector * click
        self.AInv = np.linalg.inv(self.A)
        self.UserTheta = np.dot(self.AInv, self.b)
        self.time += 1
       



    def getTheta(self):
        return self.UserTheta

    def getA(self):
        return self.A

    def decide(self, pool_articles):

        for article in pool_articles:
            if(not article.id in self.UserArmTrials ):
                self.UserArmTrials[article.id] = 1
                return article

        maxPTA = float('-inf')
        articlePicked = None


        for article in pool_articles:
            
            # confidence = np.matmul( (np.matmul(article.featureVector.T, self.AInv) ), article.featureVector)

            noise = np.sum([bernoulli.rvs(self.p) for i in range(int(self.a*self.UserArmTrials[article.id]))])

            # pick article with highest Prob

 

            article_pta = (np.dot(self.UserTheta, article.featureVector) * self.UserArmTrials[article.id] + noise) / ((self.a+1) * self.UserArmTrials[article.id])
            # pick article with highest Prob
            if maxPTA < article_pta:
                articlePicked = article
                maxPTA = article_pta


        self.UserArmTrials[articlePicked.id] += 1
        return articlePicked

class LinPHE:
    def __init__(self, dimension, lambda_, c, a, p=0.5):
        self.users = {}
        self.dimension = dimension
        self.lambda_ = lambda_
        self.c = c
        self.a = int(a) ## hyperparam
        self.p = p ## hyperparam
        self.CanEstimateUserPreference = True

    def decide(self, pool_articles, userID):
        if userID not in self.users:
            self.users[userID] = LinPHEStruct(self.dimension, self.lambda_, c= self.c, a = self.a, p=self.p)


        return self.users[userID].decide(pool_articles)

    def updateParameters(self, articlePicked, click, userID):
        self.users[userID].updateParameters(articlePicked.featureVector[:self.dimension], click)

    def getTheta(self, userID):
        return self.users[userID].UserTheta

