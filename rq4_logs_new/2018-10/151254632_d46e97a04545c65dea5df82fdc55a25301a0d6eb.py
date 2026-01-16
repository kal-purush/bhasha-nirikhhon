import json
import numpy as np
import cv2
import os
import time
import pyaudio
import wave
from datetime import datetime
import matplotlib.pyplot as plt
import concurrent.futures

def listenSNAP_withMike():
    data = stream.read(chunk)
    x = np.fft.fft(np.frombuffer(data, dtype="int16") / 32768.0)
    amp = np.abs(x)
    
    # 周波数分布に偏りが少なく、高周波域の総和が高く、最大値が低音でない音→スナップ音に近い音を抽出
    if (amp<=noize_threshold).all() and amp[450:512].sum()>snap_threshold and amp[0:512].argmax()>maxamp_threshold:
        print(amp[450:512].sum())
        return True


def getKeyPoints_fromJSON():
    frame = 0   # 実行のループをフレーム数に合わせる
    one_time = 0
    snapped = False

    while True:
        frame_time = time.time()   # ループに入った瞬間の時刻を取得

        # JSONファイルの前の数字は12桁
        index = "{0:012d}".format(frame)
        path = os.path.dirname(os.path.abspath(__file__)) + '/outputs/ByeByetest/'
        path2JSONfile = path + index + '_keypoints.json'

        if listenSNAP_withMike():
            snapped = True

        # jsonのロード
        try:
            with open(path2JSONfile, 'r') as f:
                data = json.load(f)
        except:
            continue

        kp2d = np.array(data['people'][0]['pose_keypoints_2d']).reshape((25,3))   # keypointsの取得
        kp2d[np.where(kp2d[:,2] == 0)] = (0,0,0)   # 信頼度が0の場合x座標，y座標も0にする

        kp2d = kp2d[:, 0:2]   # 信頼度は省いておく
        #print(kp2d)

        nose       = kp2d[0]
        l_eye      = kp2d[16]
        r_eye      = kp2d[15]
        l_ear      = kp2d[18]
        r_ear      = kp2d[17]
        neck       = kp2d[1]
        l_shoulder = kp2d[5]
        r_shoulder = kp2d[2]
        l_elbow    = kp2d[6]
        r_elbow    = kp2d[3]
        l_wrist    = kp2d[7]
        r_wrist    = kp2d[4]    
        mid_hip    = kp2d[8]
        l_hip      = kp2d[12]
        r_hip      = kp2d[9]
        l_knee     = kp2d[13]
        r_knee     = kp2d[10]
        l_ankle    = kp2d[14]
        r_ankle    = kp2d[11]
        l_heel     = kp2d[21]
        r_heel     = kp2d[24]
        l_bigtoe   = kp2d[19]
        r_bigtoe   = kp2d[22]
        l_smalltoe = kp2d[20]
        r_smalltoe = kp2d[23]
            
        # 偏差が必要なので2フレーム目から処理を行う
        if frame != 0: # 前の座標が0の時（検出されてない時）は外れ値として処理しよう
            if nose[0] != 0:          # 鼻が検出されている時
                if r_eye[0] != 0:     # 右目が検出されていたら鼻と右目との距離を距離（ピクセル）の基準とする
                    standard_of_distance = nose - r_eye
                elif l_eye[0] != 0:   # 右目が検出されていない時は鼻と左目との距離を距離（ピクセル）の基準とする
                    standard_of_distance = -(nose - l_eye)
            else:
                frame += 1 # JSONを更新するために必要
                continue

            delta_pixel = r_wrist - old_r_wrist
            delta_time = frame_time - old_frame_time

            delta_pixel_standardized = delta_pixel / standard_of_distance
            pixel_per_second = delta_pixel_standardized / delta_time

            # とりあえずバイバイするジェスチャ→右手首が右ひじより上にあって、内側へある程度早い速度で振った後
            # １秒以内に外側にある程度早い速度で振る
            if (r_shoulder[1]-r_wrist[1])>=0 and pixel_per_second[0] >= 20 and pixel_per_second[0] <= 50:
                gesture_flag = True
                one_time = frame_time
            if (r_shoulder[1]-r_wrist[1])>=0 and pixel_per_second[0] <= -20 and pixel_per_second[0] >= -50:
                interval = frame_time - one_time
                if interval <= 1:
                    print('Did you Bye Bye?')

        old_nose       = nose
        old_l_eye      = l_eye
        old_r_eye      = r_eye
        old_l_ear      = l_ear
        old_r_ear      = r_ear
        old_neck       = neck
        old_l_shoulder = l_shoulder
        old_r_shoulder = r_shoulder
        old_l_elbow    = l_elbow
        old_r_elbow    = r_elbow
        old_l_wrist    = l_wrist
        old_r_wrist    = r_wrist    
        old_mid_hip    = mid_hip
        old_l_hip      = l_hip
        old_r_hip      = r_hip
        old_l_knee     = l_knee
        old_r_knee     = r_knee
        old_l_ankle    = l_ankle
        old_r_ankle    = r_ankle
        old_l_heel     = l_heel
        old_r_heel     = r_heel
        old_l_bigtoe   = l_bigtoe
        old_r_bigtoe   = r_bigtoe
        old_l_smalltoe = l_smalltoe
        old_r_smalltoe = r_smalltoe

        old_frame_time = frame_time   # 次フレームまでに現在のフレームを記憶させる

        frame += 1

        if snapped == True:
            interval_of_recognizingGesture += delta_time
            print('snapped!')
        else:
            interval_of_recognizingGesture = 0

        if interval_of_recognizingGesture > 2:
            snapped = False
            print('finish snapped')


if __name__ == "__main__":
    # listenSNAP_withMike で使用する変数
    chunk = 1024
    FORMAT = pyaudio.paInt16
    CHANNELS = 1
    RATE = 44100
    RECORD_SECONDS = 2
    snap_threshold = 7
    noize_threshold = 30
    maxamp_threshold = 20
    p = pyaudio.PyAudio()
    stream = p.open(format = FORMAT,
        channels = CHANNELS,
        rate = RATE,
        input = True,
        frames_per_buffer = chunk
    )


    getKeyPoints_fromJSON()

    """
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=2)
    executor.submit(readJSON)
    executor.submit(threshold)
    print("hello")
    """

