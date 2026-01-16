import numpy as np
from matrix import Matrix, HashMatrix, cached_matmul
from mixin import MixArray

if __name__ == '__main__':
    np.random.seed(0)
    a = np.random.randint(0, 10, (10, 10))
    b = np.random.randint(0, 10, (10, 10))

    a_mat = Matrix(a.tolist())
    b_mat = Matrix(b.tolist())
    with open('artifacts/easy/matrix+.txt', 'w') as f:
        f.write(str(a_mat + b_mat))
    with open('artifacts/easy/matrix*.txt', 'w') as f:
        f.write(str(a_mat * b_mat))
    with open('artifacts/easy/matrix@.txt', 'w') as f:
        f.write(str(a_mat @ b_mat))

    a_mix = MixArray(a)
    b_mix = MixArray(b)
    with open('artifacts/medium/matrix+.txt', 'w') as f:
        f.write(str(a_mix + b_mix))
    with open('artifacts/medium/matrix*.txt', 'w') as f:
        f.write(str(a_mix * b_mix))
    with open('artifacts/medium/matrix@.txt', 'w') as f:
        f.write(str(a_mix @ b_mix))

    cache = {}
    a = HashMatrix([[1, 2], [3, 4]])
    b = HashMatrix([[1, 1], [1, 1]])
    c = HashMatrix([[4, 3], [2, 1]])
    d = HashMatrix([[1, 1], [1, 1]])
    ab = cached_matmul(a, b, cache)
    cd = cached_matmul(c, d, cache)
    cd_real = c @ d
    a.save('artifacts/hard/A.txt')
    b.save('artifacts/hard/B.txt')
    c.save('artifacts/hard/C.txt')
    d.save('artifacts/hard/D.txt')
    ab.save('artifacts/hard/AB.txt')
    cd_real.save('artifacts/hard/CD.txt')
    with open('artifacts/hard/hash.txt', 'w') as f:
        f.write(str(hash(ab)) + '\n' + str(hash(cd)))