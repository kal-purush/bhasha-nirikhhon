#!coding:utf-8

import numpy as np
import chainer
from chainer import report, training, Chain, datasets, iterators, optimizers,cuda,serializers
import chainer.functions as F
import chainer.links as L
from chainer.training import extensions
from chainer.datasets import tuple_dataset
import time
import cv2 as cv

class vehicle_classify_CNN(Chain):
    def __init__(self):
        super(vehicle_classify_CNN,self).__init__(
            conv1=L.Convolution2D(3,20,7),
            conv2=L.Convolution2D(20,8,4),
            conv3=L.Convolution2D(8,8,4),
            fc = L.Linear(72,2)
        )
    def __call__(self,x):
        h1 = F.max_pooling_2d(F.relu(self.conv1(x)),2,2)
        h2 = F.max_pooling_2d(F.relu(self.conv2(h1)),2,2)
        h3 = F.max_pooling_2d(F.relu(self.conv3(h2)),2,2)
        y = self.fc(h3)
        return y


def main():
    exec_time = time.time()
    modelload = True

    model = L.Classifier(vehicle_classify_CNN())
    optimizer = optimizers.SGD()
    if modelload:serializers.load_npz("gradient_cnn.npz", model)
    optimizer.setup(model)
    if modelload:serializers.load_npz("gradient_optimizer.npz", optimizer)
    model.to_gpu()

    data = np.load("data.npy")
    val = np.load("val.npy")
    mean_image = np.load("mean_image.npy")

    windows = np.load("windows.npy")
    windows = windows[0:100]
    windows -=mean_image

    data -= mean_image

    # for i in range(20):
    #     print(data[i])
    #     print(data[i].shape)
    #     print(val[i])
    #     cv.imshow("test",data[i].transpose(1,2,0))
    #     cv.waitKey(0)
    #     cv.destroyAllWindows()

    print(data.size/48/48/3)
    print(val.size)

    # N_ = 9500
    # data, testdata = np.split(data,[N_])
    # val, testval = np.split(val,[N_])

    N = 18000
    data_train ,data_test = np.split(data,[N])
    val_train , val_test = np.split(val,[N])

    train = tuple_dataset.TupleDataset(data_train,val_train)
    test = tuple_dataset.TupleDataset(data_test,val_test)

    train_iter = iterators.SerialIterator(train,batch_size=100)
    test_iter = iterators.SerialIterator(test,batch_size=50,repeat=False,shuffle=False)

    updater = training.StandardUpdater(train_iter,optimizer,device=0)
    trainer = training.Trainer(updater,(2000,"epoch"),out = "result")

    trainer.extend(extensions.Evaluator(test_iter,model,device=0))
    trainer.extend(extensions.LogReport())
    trainer.extend(extensions.PrintReport(["epoch","main/accuracy","validation/main/accuracy"]))
    trainer.extend(extensions.ProgressBar())

    trainer.run()

    print("predict:",model.predictor(cuda.to_gpu(windows)).data)
    print("probability:",F.softmax(model.predictor(cuda.to_gpu(windows)).data).data)
    print("label:",F.softmax(model.predictor(cuda.to_gpu(windows)).data).data.argmax(axis=1))
    print(type(F.softmax(model.predictor(cuda.to_gpu(windows)).data).data.argmax(axis=1)))
    #print("valid:",testval)

    model.to_cpu()
    serializers.save_npz("gradient_cnn.npz", model)
    serializers.save_npz("gradient_optimizer.npz", optimizer)

    exec_time = time.time() - exec_time
    print("exex time:%f"% exec_time)

if __name__ == "__main__":
    main()