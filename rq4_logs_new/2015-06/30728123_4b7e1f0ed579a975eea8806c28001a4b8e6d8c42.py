"""Custom reference transition matrix: 3 state."""

from msmbuilder.msm import MarkovStateModel
import numpy as np


def get_mat(go_uphill, stay_uphill):
    go_down = (1.0 - stay_uphill) / 2
    stay_big = 1.0 - go_uphill
    return np.asarray([
        [stay_big, go_uphill, 0],
        [go_down, stay_uphill, go_down],
        [0, go_uphill, stay_big]
    ])


def get_ref_msm(go_uphill, stay_uphill):
    msm = MarkovStateModel()
    mat = get_mat(go_uphill, stay_uphill)
    msm.transmat_, msm.populations_ = msm._fit_mle(mat)
    msm._is_dirty = True
    return msm