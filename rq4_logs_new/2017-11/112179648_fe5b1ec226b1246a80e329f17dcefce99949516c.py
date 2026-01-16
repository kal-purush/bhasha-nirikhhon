import pandas as pd

from data import tweets
from features.feature_builder import FeatureBuilder
from features import dimensionality_reduction
from model.kmeans import KM
from util.tokenize import remove_urls
from util import log, constants

logger = log.logger


def get_data():
    real = tweets.load_directory(constants.REAL_TWEETS_DIR, constants.DELIMITER)
    fake = tweets.load_csv(constants.FAKE_TWEETS_FILE, constants.DELIMITER)

    for df in real:
        df['real'] = 1
    fake['real'] = 0

    logger.debug("First real tweet from %s: %s" % (real[0]['label'].iloc[0],
                                                real[0]['text'].iloc[0]))
    logger.debug("First fake tweet from %s: %s" % (fake['label'].iloc[0],
                                                   fake['text'].iloc[0]))

    # real.append(fake)
    return pd.concat(real)

def cluster():
    '''
        Cluster the samples together and produce graphs to show the result

    :return:
    '''

    total_samples = get_data()

    # Get labels and number of unique labels
    labels = total_samples[['label']]
    logger.debug("Labels shape: (%s, %s)" % labels.shape)
    real_k = len(labels['label'].unique())

    # Create features from samples
    X = total_samples.drop(['label', 'real'], axis=1)
    X['text'] = X['text'].apply(lambda t: remove_urls(t)) # Remove any urls
    feature_builder = FeatureBuilder(X, constants.WORD_N, constants.CHAR_N)
    X = feature_builder.featurize_all(X)

    logger.debug("n_samples: %d, n_features: %d" % X.shape)

    # Reduce domensionality (Curse of Dimensionality)
    X = dimensionality_reduction.reduce(X, 5000) # TODO parameteruze n_components

    logger.debug("After LSA shape: %d samples, %d features" % X.shape)



    # Create the model and fit the data
    model = KM(feature_builder, real_k)
    model.fit(X, labels.values.reshape((-1)))

cluster()