import numpy as np


def bbox_overlap(boxes, query_boxes):
    """
    Parameters
    ----------
    boxes: (N, 4) ndarray of float
    query_boxes: (K, 4) ndarray of float
    Returns
    -------
    overlaps: (N, K) ndarray of overlap between boxes and query_boxes
    -------
    This is a substitution of cython_bbox as a pure python implementation.
    Don't expect it to be fast.
    """
    N = boxes.shape[0]
    K = query_boxes.shape[0]
    overlaps = np.zeros((N, K), dtype=np.float)
    for k in range(K):
        qbox_area = (query_boxes[k, 2] - query_boxes[k, 0] + 1) * \
            (query_boxes[k, 3] + query_boxes[k, 1] + 1)
        for n in range(N):
            iw = min(boxes[n, 2], query_boxes[k, 2]) - \
                max(boxes[n, 0], query_boxes[k, 0]) + 1
            if iw > 0:
                ih = min(boxes[n, 3], query_boxes[k, 3]) - \
                    max(boxes[n, 1], query_boxes[k, 1]) + 1
                if ih > 0:
                    # overlap, compute overlap area
                    inter = iw * ih
                    box_area = (boxes[n, 2] - boxes[n, 0] + 1) * \
                        (boxes[n, 3] - boxes[n, 1] + 1)
                    overlaps[n, k] = inter / (qbox_area + box_area - inter)