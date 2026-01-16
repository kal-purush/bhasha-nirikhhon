import scipy.io
import h5py
import numpy as np

if __name__ == "__main__":
    mat_path = "../data/perspective-ShanghaiTech/A/train_pmap/IMG_1.mat"
    # mat = scipy.io.loadmat(mat_path)
    # print(mat)
    with h5py.File(mat_path, 'r') as f:
        target = np.asarray(f['pmap'])
        print(target.shape)
        print(target)