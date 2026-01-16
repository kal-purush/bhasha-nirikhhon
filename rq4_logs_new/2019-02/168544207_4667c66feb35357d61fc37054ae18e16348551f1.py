# !/usr/bin/python
# -*- coding: utf-8 -*-
import Config
import math

class MetricUtil(object):
    """docstring for MetricUtil"""
    def __init__(self):
        super(MetricUtil, self).__init__()

    def calculate_study_threshold(self, data):
        ''''''
        res = []
        for key, value in data.items():
            res.append(value['face_pose_normal'])

        res.sort(reverse=True)
        length = len(data)

        threshold = {}
        # 不佳
        threshold['study_bad'] = res[math.floor(length * Config.STUDY_THREHOLD_BAD['FACE_POSE_NORMAL'])]
        # 非常好
        threshold['study_great'] = res[math.floor(length * Config.STUDY_THREHOLD_GREAT['FACE_POSE_NORMAL'])]
        # 良好
        threshold['study_good'] = res[math.floor(length * Config.STUDY_THREHOLD_GOOD['FACE_POSE_NORMAL'])]

        return threshold

    def estimate_study_stat(self, mentals, face_poses, thresholds):
        ''''''
        if mentals['student_mental_stat'] == Config.STUDY_THREHOLD_BAD['MENTAL'] and face_poses['face_pose_normal'] < thresholds['study_bad']: # 学习状态 -- 不佳
            return 3
        elif mentals['student_mental_stat'] == Config.STUDY_THREHOLD_GREAT['MENTAL'] and face_poses['face_pose_normal'] > thresholds['study_great']: # 学习状态 -- 非常好
            return 0
        elif mentals['student_mental_stat'] in Config.STUDY_THREHOLD_GOOD['MENTAL'] and face_poses['face_pose_normal'] > thresholds['study_good']: # 学习状态 -- 良好
            return 1
        else: # 学习状态 -- 正常
            return 2

    def estimate_mental_stat(self, emotion, body_stat):
        ''''''
        if ((emotion['emotion_happy'] < Config.MENTAL_THRESHOLD_TIRED['EMOTION_SMILE'] \
            or emotion['emotion_low'] > Config.MENTAL_THRESHOLD_TIRED['EMOTION_LOW']) \
        and (body_stat['body_stat_sttk'] > Config.MENTAL_THRESHOLD_TIRED['BODY_STAT'] \
        or body_stat['body_stat_pztk'] > Config.MENTAL_THRESHOLD_TIRED['BODY_STAT'])): # 精神状态 -- 疲惫
            return 2
        elif (emotion['emotion_happy'] > Config.MENTAL_THRESHOLD_POSITIVE['EMOTION_SMILE'] \
        and (body_stat['body_stat_sttk'] > Config.MENTAL_THRESHOLD_POSITIVE['BODY_STAT'] \
        or body_stat['body_stat_pztk'] > Config.MENTAL_THRESHOLD_POSITIVE['BODY_STAT'])): # 精神状态 -- 积极
            return 0
        else: # 正常
            return 1