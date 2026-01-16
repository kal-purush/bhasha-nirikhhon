import numpy as np

def nonlin(x, deriv=False): #function definition of the sigmoid function
        return (x*(1-x))
    
    return 1/(1+np.exp(-x))


class RedeNeural:

    def __init__(sefl, eta, numrounds, hidden_nodes): #Construtor
        self.numrounds = numrounds
        '''
        fit roda até que numrounds
        seja alcançado
        '''
        
        self.eta = eta #learning rate
        self.hidden_nodes = hidden_nodes
        '''
        Quantidade de nós no hidden layer
        '''
        
    def fit(self, X, y):
        pass
        #fit não deve possuir retorno.
        '''
        o fit deve descobrir a dimensão do problema
        e o número de classes.
        '''
        
    def predict(self, X):
        pass
        #predict deve retornar as classes para X