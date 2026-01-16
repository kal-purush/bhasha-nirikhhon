import sklearn.decomposition.PCA as PCA 


class NoSignalException(Exception):
    pass

class NotTrainedException(Exception):
    pass

class PCAModel(object):

    def __init__(self):
        self.model = None
        self.events = {
            "trained": self.nothing,
            "validated": self.nothing,
            "classified": self.nothing
        }

    def train(self, data):
        """
        data: list of dicts with 'label' and 'sample'
        """
        self.model = PCA(n_component=None)
        n_features = len(data[0]["sample"])
        n_samples = len(data)
        self.training = np.zeros([n_samples, n_features])
        for i in range(n_samples):
            for j in range(n_features):
                self.training[i,j] = data[i]["sample"][j]
        self.model.fit(self.training)
        self.events["trained"](eigenvectors=self.model.components_, eigenvalues=[], mean=self.model.mean_, data=data)

    def classify(self, sample):
        if self.model == None:
            raise NotTrainedException()
        # TODO 

    def validate(self, validation_data):
        pass

    def project(self, sample):
        pass

    def get_eigenvectors(self):
        pass

    def get_mean(self):
        return self.model.mean_

    def connect(self, event, fun):
        """
        Connect to signal emmited by model 

        Signals can be:

        trained: function gets keyword arguments 'eigenvectors', 'eigenvalues', 'mean' and 'data' where each element is dict {'label', 'sample'}
        validated: function gets keyword argument 'precision', 'accuracy' and 'validation_data' where each element is dict {'label', 'sample'}
        classified: function with keyword argument 'sample' and 'label' 
        """
        if not event in self.events:
            raise NoSignalException()
        else:
            self.events[event] = fun
    
    def nothing(self, **kwargs):
        pass
