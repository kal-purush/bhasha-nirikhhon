import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Function to calculate precision
# model_src : source language embeddings (need to have syn0 and index2word properites) (after translation)
# model_tar : target language embeddings (need to have syn0 and index2word properites) (can be don in orig or universal space)
# dict_scr_2_tar : dictionary from source to target
def calc_precision(precs, model_src, model_tar, dict_scr_2_tar, logger):
    W_src = model_src.syn0
    W_tar = model_tar.syn0
    idx_src = model_src.index2word
    idx_tar = model_tar.index2word

    cos_mx = cosine_similarity(W_src, W_tar)
    sim_mx = np.argsort(-cos_mx)
    max_prec = max(precs)
    prec_cnt = np.zeros(shape=(1, max_prec))
    logger.debug('word: \ttranslations in dict: \tclosest words after translation: \t')
    for i, r in enumerate(sim_mx):
        key_word = idx_src[i]
        value_words = dict_scr_2_tar[key_word]
        closest_words = []
        for j in range(max_prec):
            ans = np.where(r == j)
            idx_orig = ans[0][0]
            word = idx_tar[idx_orig]
            closest_words.append(word)
            if word in value_words:
                prec_cnt[0][j] = prec_cnt[0][j] + 1
        logger.debug('{}"\t{}\t{}'.format(key_word, value_words, closest_words))
    logger.debug(prec_cnt)
    prec_pcnts = []
    for i, val in enumerate(precs):
        sum_hit = np.sum(prec_cnt[0][0:val])
        pcnt = float(sum_hit) / sim_mx.shape[0]
        logger.debug('prec {} : {}'.format(val, pcnt))
        prec_pcnts.append(pcnt)
    return prec_pcnts