from typing import List

import numpy as np


def solve(a: List[List[float]], b: List[float]) -> List[float]:
    """
    >>> solve([[3,1], [1,2]], [9, 8])
    [2.0, 3.0]
    """
    return list(np.linalg.solve(np.array(a), np.array(b)))
