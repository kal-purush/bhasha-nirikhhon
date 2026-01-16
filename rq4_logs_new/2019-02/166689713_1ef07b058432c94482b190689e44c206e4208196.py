from copy import copy

from typing import List
import random

from intervallum.box import Box, BoxVector
from intervallum.interval_functions import split as i_split


def split(b: BoxVector, ratios: List[float], split_id: int = None) -> List[BoxVector]:
    if split_id is None:
        ids, w = b.width
        id_ = random.choice(ids)
    else:
        id_ = split_id

    part_1 = [copy(i) for i in b[:id_]]
    part_2 = [copy(i) for i in b[(id_ + 1):]]

    boxes = [Box(*(part_1 + [i] + part_2)) for i in i_split(b[id_], ratios)]
    boxes = [b._try_to_reduce() for b in boxes]
    return boxes


def bisect(b: BoxVector, split_id: int = None) -> List[BoxVector]:
    return split(b, [1.0, 1.0], split_id)