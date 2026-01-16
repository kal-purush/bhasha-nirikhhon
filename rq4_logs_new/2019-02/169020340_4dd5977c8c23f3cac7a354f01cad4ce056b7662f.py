# -*- coding:utf-8 -*-
import cv2
import numpy as np
import skimage
import caffe
from caffe.proto import caffe_pb2
caffe.set_mode_gpu()


# 検出用パーティクルフィルタ
class DetectingParticleFilter:
    # パーティクルの定義
    def __init__(self, SAMPLEMAX, upper_x, upper_y, lower_x, lower_y):
        # パーティクルの数
        self.SAMPLEMAX = SAMPLEMAX
        # パーティクルが取りうる座標の上限値
        self.upper_x, self.upper_y = upper_x, upper_y
        self.lower_x, self.lower_y = lower_x, lower_y
        self.upper_w, self.upper_h = 150, 150
        self.lower_w, self.lower_h = 32, 32
        # カウンタ
        self.count1 = 0
        self.count2 = 0
        self.count3 = 0

    # パーティクルの初期化関数
    def initialize(self):
        self.W = np.random.random(self.SAMPLEMAX) * (self.upper_w - self.lower_w) + self.lower_w
        self.H = np.random.random(self.SAMPLEMAX) * (self.upper_h - self.lower_h) + self.lower_h
        self.X = np.random.random(self.SAMPLEMAX) * (self.upper_x - self.lower_x) + (self.lower_x - self.W/2)
        self.Y = np.random.random(self.SAMPLEMAX) * (self.upper_y - self.lower_y) + (self.lower_y - self.H/2)

    # 状態遷移関数
    def modeling(self):
        # ランダムウォーク
        self.X += np.random.random(self.SAMPLEMAX) * 20 - 10
        self.Y += np.random.random(self.SAMPLEMAX) * 20 - 10
        self.W += np.random.random(self.SAMPLEMAX) * 20 - 10
        self.H += np.random.random(self.SAMPLEMAX) * 20 - 10
        # 上限、下限を超えないよう設定
        for i in range(self.SAMPLEMAX):
            if (self.X[i] + self.W[i]) > self.upper_x:
                self.W[i] = self.upper_x - self.X[i]
            if (self.Y[i] + self.H[i]) > self.upper_y:
                self.H[i] = self.upper_y - self.Y[i]
            if self.X[i] > self.upper_x - self.lower_w:
                self.X[i] = self.upper_x - self.lower_w
                self.W[i] = self.lower_w - 1
            if self.Y[i] > self.upper_y - self.lower_h:
                self.Y[i] = self.upper_y - self.lower_h
                self.H[i] = self.lower_h - 1
            if self.W[i] > self.upper_w: self.W[i] = self.upper_w
            if self.H[i] > self.upper_h: self.H[i] = self.upper_h
            if self.X[i] < self.lower_x: self.X[i] = self.lower_x
            if self.Y[i] < self.lower_y: self.Y[i] = self.lower_y
            if self.W[i] < self.lower_w: self.W[i] = self.lower_w
            if self.H[i] < self.lower_h: self.H[i] = self.lower_h

    # caffeの設定
    def caffe_preparation(self):
        mean_blob = caffe_pb2.BlobProto()
        with open('../model/crow_mean.binaryproto') as f:
            mean_blob.ParseFromString(f.read())
        mean_array = np.asarray(
        mean_blob.data,
        dtype = np.float32).reshape(
        	(mean_blob.channels,
        	mean_blob.height,
        	mean_blob.width))
        self.classifier = caffe.Classifier(
            '../model/crow.prototxt',
            '../model/crow_iter_100000.caffemodel',
            mean=mean_array,
            raw_scale=255)

    # 尤度関数
    def calcLikelihood(self, image):
        self.class0_count = 0 # クラス0である確率が一番高かった回数
        intensity = []        # 尤度を格納する配列
        # パーティクルごとに尤度を取得
        for i in range(self.SAMPLEMAX):
            # パーティクルの領域画像を取得
            y, x, w, h = self.Y[i], self.X[i], self.W[i], self.H[i]
            roi = image[y:y+h, x:x+w]
            # 領域画像をcaffeのフォーマットに合わせる
            img = cv2.cvtColor(roi, cv2.COLOR_BGR2RGB)
            img = skimage.img_as_float(img).astype(np.float32)
            # caffe識別器が確率を出力
            predictions = self.classifier.predict([img], oversample=False)
            argmax_class = np.argmax(predictions)
            if argmax_class == 0:
                self.class0_count += 1
            intensity.append(predictions[0][int(0)]) # 対象物体クラスの確率を尤度として配列に保存
        weights = self.normalize(intensity)          # 正規化関数からweights(重み)を取得
        # 強制初期化
        if self.class0_count < 20:
            self.count1 += 1
            if self.count1 == 10:
                self.initialize()
                self.count1 = 0
        else:
            self.count1 = 0
        return weights

    # 正規化関数
    def normalize(self, predicts):
        return predicts / np.sum(predicts)

    # リサンプリング関数
    def resampling(self, weight):
        sample = [] # リストの用意
        index = np.arange(self.SAMPLEMAX) # 引数の用意
        # 重みが大きいパーティクルの引数が確率的に多く選択される
        for i in range(self.SAMPLEMAX):
            idx = np.random.choice(index, p=weight)
            sample.append(idx)
        return sample

    # 追跡関数
    def filtering(self, image):
        self.flag = False # フラグ
        self.modeling()   # 状態遷移関数の呼び出し
        weights = self.calcLikelihood(image) # 尤度関数から重みと尤度の平均値を取得
        index = self.resampling(weights)     # リサンプリング関数から引数を取得
        # パーティクルの更新
        self.X = self.X[index]
        self.Y = self.Y[index]
        self.W = self.W[index]
        self.H = self.H[index]
        # 対象推定
        px, py, pw, ph = 0, 0, 0, 0
        px = np.average(self.X, weights = weights)
        py = np.average(self.Y, weights = weights)
        pw = np.average(self.W, weights = weights)
        ph = np.average(self.H, weights = weights)
        prob = self.class0_count / self.SAMPLEMAX
        # フラグの判定
        if self.class0_count > self.SAMPLEMAX * 0.7:
            print self.class0_count
            self.count3 += 1
            if self.count3 == 3:
                self.flag = True
                self.count3 = 0
        else:
            print self.class0_count
            self.count3 = 0
            self.count2 += 1
            if self.count2 == 20:
                self.initialize()
                self.count2 = 0
        return px, py, pw, ph, prob