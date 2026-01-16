"""Functions for NumPy arrays that loop like gufuncs.

    There is a general need for looping over not only functions on
    scalars but also over functions on vectors (or arrays). This concept
    is realized in NumPy by generalizing the universal functions
    (ufuncs). In regular ufuncs, the elementary function is limited to
    element-by-element operations, whereas the generalized version
    (gufuncs) supports "sub-array" by "sub-array" operations. The Perl
    vector library PDL provides a similar functionality and its terms
    are re-used in the following.

    Each generalized ufunc has information associated with it that
    states what the "core" dimensionality of the inputs is, as well as
    the corresponding dimensionality of the outputs (the element-wise
    ufuncs have zero core dimensions). The list of the core dimensions
    for all arguments is called the "signature" of a ufunc. For example,
    the ufunc numpy.add has signature (),()->() defining two scalar
    inputs and one scalar output.

    Another example is the function inner1d(a, b) with a signature of
    (i),(i)->(). This applies the inner product along the last axis of
    each input, but keeps the remaining indices intact. For example,
    where a is of shape (3, 5, N) and b is of shape (5, N), this will
    return an output of shape (3,5). The underlying elementary function
    is called 3 * 5 times. In the signature, we specify one core
    dimension (i) for each input and zero core dimensions () for the
    output, since it takes two 1-d arrays and returns a scalar. By using
    the same name i, we specify that the two corresponding dimensions
    should be of the same size.

    The dimensions beyond the core dimensions are called "loop"
    dimensions. In the above example, this corresponds to (3, 5).

    -- https://numpy.org/doc/stable/reference/c-api/generalized-ufuncs.html

Functions
---------
stack
    Stack arrays, broadcasting over their dimensions.
concatenate
    Concatenate arrays along their innermost dimension.
pad_indices
    Pad indices with arrays for the outermost dimensions of a shape.
get
    Index an array's innermost dimensions, broadcasting over the others.
"""

import numpy as np


def stack(arrays, out=None, *, casting="same_kind", dtype=None):
    """Stack arrays, broadcasting over their dimensions.

    - lambda: stack([]) has signature ->(0);
    - lambda a: stack([a]) has signature ()->(1);
    - lambda a, b: stack([a, b]) has signature (),()->(2);

    and so on.

    >>> import gufunky
    >>> gufunky.stack([0, [1, 2]])
    array([[0, 1],
           [0, 2]])
    """
    arrays = list(arrays)
    if arrays:
        broadcasts = np.broadcast_arrays(*arrays)
        return np.stack(broadcasts, axis=-1, out=out, casting=casting, dtype=dtype)
    return np.array([], dtype)


def concatenate(arrays, out=None, *, casting="same_kind", dtype=None):
    """Concatenate arrays along their innermost dimension.

    - lambda: concatenate([]) has signature ->(0);
    - lambda a: concatenate([a]) has signature (n)->(n);
    - lambda a, b: concatenate([a, b]) has signature (n),(o)->(n+o);

    and so on.

    >>> import gufunky
    >>> gufunky.concatenate([[0, 1], [[2], [3]]])
    array([[0, 1, 2],
           [0, 1, 3]])
    """
    arrays = list(map(np.asanyarray, arrays))
    if arrays:
        loop = np.broadcast_shapes(*(a.shape[:-1] for a in arrays))
        broadcasts = [np.broadcast_to(a, loop + a.shape[-1:]) for a in arrays]
        return np.concatenate(
            broadcasts, axis=-1, out=out, casting=casting, dtype=dtype
        )
    return np.array([], dtype)


def pad_indices(shape, indices):
    """Pad indices with arrays for the outermost dimensions of shape.

    >>> import numpy as np
    >>> import gufunky
    >>> a = np.arange(6).reshape((3, 2))
    >>> a
    array([[0, 1],
           [2, 3],
           [4, 5]])
    >>> indices = gufunky.pad_indices(a.shape, ([1, 0, 1],))
    >>> indices
    (array([0, 1, 2]), [1, 0, 1])
    >>> a[indices] += 10
    >>> a
    array([[ 0, 11],
           [12,  3],
           [ 4, 15]])
    """
    return np.indices(shape[: len(shape) - len(indices)], sparse=True) + indices


def get(a, indices):
    """Index with indices a's innermost dimensions, broadcasting over the others.

    - lambda a: get(a, ()) has signature ()->();
    - lambda a, z: get(a, (z,)) has signature (m),()->();
    - lambda a, y, z: get(a, (y, z)) has signature (l,m),(),()->();

    and so on.

    >>> import numpy as np
    >>> import gufunky
    >>> a = np.arange(6).reshape((3, 2))
    >>> a
    array([[0, 1],
           [2, 3],
           [4, 5]])
    >>> gufunky.get(a, ([1, 0, 1],))
    array([1, 2, 5])
    >>> gufunky.get(a, ([[1, 0, 1], [0, 0, 1], [1, 1, 0], [0, 0, 0]],))
    array([[1, 2, 5],
           [0, 2, 5],
           [1, 3, 4],
           [0, 2, 4]])
    >>> gufunky.get(a, ())
    array([[0, 1],
           [2, 3],
           [4, 5]])
    >>> gufunky.get(a, (2, [1, 0]))
    array([5, 4])
    >>> b = np.arange(24).reshape((4, 3, 2))
    >>> b
    array([[[ 0,  1],
            [ 2,  3],
            [ 4,  5]],
    <BLANKLINE>
           [[ 6,  7],
            [ 8,  9],
            [10, 11]],
    <BLANKLINE>
           [[12, 13],
            [14, 15],
            [16, 17]],
    <BLANKLINE>
           [[18, 19],
            [20, 21],
            [22, 23]]])
    >>> gufunky.get(b, ([1, 0, 1],))
    array([[ 1,  2,  5],
           [ 7,  8, 11],
           [13, 14, 17],
           [19, 20, 23]])
    >>> gufunky.get(b, ([1, 1, 0, 2], 1))
    array([ 3,  9, 13, 23])
    """
    a = np.asanyarray(a)
    return a[pad_indices(a.shape, indices)]