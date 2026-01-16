from util import log

logger = log.logger

class BaseFeatureBuilder():
    def __init__(self):
        pass

    def featurize_all(self, samples):
        '''
        The intended way this method should work:
            Samples is expected as a Pandas DataFrame
            The return type should be a Scipy csr sparse matrix

        :param samples:
        :return:
        '''
        raise NotImplementedError

    def featutize(self, text):
        '''
            This method should turn a single string into a Scipy csr sparse matrix
            This method will be used by `featurize_all()`
        :param text:
        :return:
        '''
        raise NotImplementedError