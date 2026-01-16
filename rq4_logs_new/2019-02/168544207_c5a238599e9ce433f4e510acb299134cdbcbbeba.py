# !/usr/bin/python
# -*- coding: utf-8 -*-
import Config
import DbUtil
import Logger
import math
import CommonUtil

class CalcCourseMetric(object):
    """docstring for CalcCourseMetric"""
    def __init__(self):
        super(CalcCourseMetric, self).__init__()
        self.__db = DbUtil(Config.INPUT_DB_HOST, Config.INPUT_DB_USERNAME, Config.INPUT_DB_PASSWORD, Config.INPUT_DB_DATABASE, Config.INPUT_DB_CHARSET)
        self.__logger = Logger(__name__)

    def calculate_course_metrics(self, start_time, end_time):
        """
            Calculate 2 metrics, including student_study_stat and student_mental_stat for each course for each student.
            return {
                'face_id1' => {
                    'course_name1' => {'student_study_stat' => value, 'student_mental_stat' => value}, 
                    'course_name2' => {'student_study_stat' => value, 'student_mental_stat' => value}
                },
                'face_id2' => {
                    'course_name1' => {'student_study_stat' => value, 'student_mental_stat' => value}, 
                    'course_name2' => {'student_study_stat' => value, 'student_mental_stat' => value}
                }
            }
        """

        # Compute the count of each basic metric
        self.__logger.info("Tring to compute course basic metrics")
        body_stat_count = self.count_body_stat(start_time, end_time)
        emotion_count = self.count_face_emotion(start_time, end_time)
        face_pose_count = self.count_face_pose(start_time, end_time)
        self.__logger.info("Finished to compte course basic metrcis")


        self.__logger.info("Tring to comput the course metrics")
        metrcis = {}
        # Calculate student mental status based on body_stat and emotion
        for course_name, values in emotion_count.items():
            for face_id, value in values.items():
                if body_stat_count.has_key(course_name) and body_stat_count[course_name].has_key(face_id):
                    if not metrics.has_key(face_id):
                        metrics[face_id] = {}

                    if not metrics[face_id].has_key(course_name):
                        metrics[face_id][course_name] = {}

                    metrcis[face_id][course_name]['student_mental_stat'] = self.estimate_mental_stat(value, body_stat_count[course_name][face_id])

        # Calculate student study status based on student mental and face_pose
        # Calculate threshold
        for course_name, values in face_pose_count:
            study_stat_thresholds = self.calculate_study_threshold(face_pose_count[course_name])
            for face_id in metrcis:
                if face_pose_count[course_name].has_key(face_id):
                    metrcis[face_id][course_name]['student_study_stat'] = self.estimate_study_stat(metrcis[face_id][course_name], face_pose_count[course_name][face_id], study_stat_thresholds)
        self.__logger.info("Finished to compute the course metrics")

        return metrcis

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

    def count_body_stat(self, start_time, end_time):
        ''''''
        sql = '''
            SELECT
                course_name, face_id, body_stat, COUNT(*) AS total
            FROM {2}
            WHERE pose_stat_time >= {0} AND pose_stat_time <= {1} AND body_stat != -1 AND face_id != 'unknown'
            GROUP BY course_name, face_id, body_stat
        '''.format(start_time, end_time, Config.INTERMEDIATE_TABLE_TRAIN)

        res = {} # {'course_name' => {'face_id1' => values, 'face_id2' => values}}
        for row in self.__db.select(sql):
            if not res.has_key(row[0]):
                res[row[0]] = {}

            if not res[row[0]].has_key(row[1]):
                res[row[0]][row[1]] = {}

            if row[2] == 0: # 正常
                res[row[0]][row[1]]['body_stat_normal'] = row[3]
            elif row[2] == 1: # 站立
                res[row[0]][row[1]]['body_stat_standup'] = row[3]
            elif row[2] == 2: # 举手
                res[row[0]][row[1]]['body_stat_handup'] = row[3]
            elif row[2] == 3: # 睡觉
                res[row[0]][row[1]]['body_stat_sleep'] = row[3]
            elif row[2] == 4: # 手托着听课
                res[row[0]][row[1]]['body_stat_sttk'] = row[3]
            elif row[2] == 5: # 趴着听课
                res[row[0]][row[1]]['body_stat_pztk'] = row[3]
            else:
                continue

        return res

    def count_face_emotion(self, start_time, end_time):
        ''''''
        sql = '''
            SELECT
                course_name, face_id, face_emotion, COUNT(*) AS total
            FROM {2}
            WHERE pose_stat_time >= {0} AND pose_stat_time <= {1} AND face_emotion != -1 AND face_id != 'unknown'
            GROUP BY course_name, face_id, face_emotion
        '''.format(start_time, end_time, Config.INTERMEDIATE_TABLE_TRAIN)

        res = {}
        for row in self.__db.select(sql):
            if not res.has_key(row[0]):
                res[row[0]] = {}

            if not res[row[0]].has_key(row[1]):
                res[row[0]][row[1]] = {}

            if row[2] == 0: # 正常
                res[row[0]][row[1]]['emotion_normal'] = row[3]
            elif row[2] == 1: # 开心
                res[row[0]][row[1]]['emotion_happy'] = row[3]
            elif row[2] == 2: # 低落
                res[row[0]][row[1]]['emotion_low'] = row[3]
            else:
                continue

        return res

    def count_face_pose(self, start_time, end_time):
        ''' Compute the count of face pose based on face_id level '''
        sql = '''
            SELECT
                course_name, face_id, face_pose, COUNT(*) AS total
            FROM {2}
            WHERE pose_stat_time >= {0} AND pose_stat_time <= {1} AND face_pose != -1 AND face_pose_stat == 0 AND face_id != 'unknown'
            GROUP BY course_name, face_id, face_pose
        '''.format(start_time, end_time, Config.INTERMEDIATE_TABLE_TRAIN)

        res = {}
        for row in self.__db.select(sql):
            if not res.has_key(row[0]):
                res[row[0]] = {}

            if not res[row[0]].has_key(row[1]):
                res[row[0]][row[1]] = {}

            if row[2] == 0: # 平视
                res[row[0]][row[1]]['face_pose_normal'] = row[3]
            elif row[2] == 1: # 左顾右盼
                res[row[0]][row[1]]['face_pose_around'] = row[3]
            elif row[2] == 2: # 低头
                res[row[0]][row[1]]['face_pose_low'] = row[3]
            else:
                continue

        return res

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