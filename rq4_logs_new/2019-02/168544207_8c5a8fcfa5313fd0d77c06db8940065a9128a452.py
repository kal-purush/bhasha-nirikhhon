# !/usr/bin/python
# -*- coding: utf-8 -*-

import Config
import DbUtil
import Logger
from CommonUtil import CommonUtil

class AnalyzeGrade(object):
    """Analyze grade with course ans study_status"""
    def __init__(self):
        super(AnalyzeGrade, self).__init__()
        self.__db = DbUtil.DbUtil(Config.INPUT_DB_HOST, Config.INPUT_DB_USERNAME, Config.INPUT_DB_PASSWORD, Config.INPUT_DB_DATABASE, Config.INPUT_DB_CHARSET)
        self.__logger = Logger.Logger(__name__)

    def Analysis(self, start_time, end_time):
        ''''''
        self.__logger.info("Begin to analyze grade and study_status")
        grade_levels = self.get_course_grade_level(start_time, end_time)
        if len(grade_level) == 0: # 这个时间段无考试成绩 无需评估
            return {}

        study_levels = self.get_course_study_status(CommonUtil.get_specific_unixtime(end_time, Config.ANALYSIS_LOOKBACKWINDOW) , end_time)

        metrics = {}
        for stu, courses in study_levels.items():
            for course_name, value in courses.items():
                if grade_level.has_key(stu) and grade_level[stu].has_key(course_name):
                    if not metrics.has_key(stu):
                        metrics[stu] = []

                    metrics[stu].append([course_name, grade_levels[stu][course_name], value])

        self.__logger.debug(str(metrics))
        self.__logger.info("Finished to analyze grade and study_status")
        return metrics

    def get_course_grade_level(self, start_time, end_time):
        ''''''
        self.__logger.info("Try to get all course_names from {0} between {1} and {2}".format(Config.SCHOOL_PERFORMANCE_TABLE, start_time, end_time))
        sql = '''
            SELECT
                t0.grade_name, t0.class_name, t0.student_number, t0.course_name, ROUND(1.0 * (t0.score - t3.avg_score) / (IF(t0.score <= t3.avg_score, t3.avg_score, t3.high_score)), 2) AS grade_level
            FROM
            (
                SELECT
                    grade_name, class_name, student_number, course_name, score
                FROM {2}
                WHERE dt >= {0} AND dt <= {1}
            ) t0 JOIN
            (
                SELECT
                    t1.grade_name, t1.class_name, t1.course_name, t1.avg_score, t2.max_score, (t2.max_score - t1.avg_score) AS high_score
                FROM
                (
                    (
                        SELECT
                            grade_name, class_name, course_name, AVG(score) AS avg_score
                        FROM {2}
                        WHERE dt >= {0} AND dt <= {1}
                        GROUP BY grade_name, class_name, course_name
                    ) t1 JOIN
                    (
                        SELECT
                            grade_name, course_name, MAX(score) AS max_score
                        FROM {2}
                        WHERE dt >= {0} AND dt <= {1}
                        GROUP BY grade_name, course_name
                    ) t2 ON t1.grade_name = t2.grade_name AND t1.course_name = t2.course_name
                )
            ) t3 ON t0.grade_name = t3.grade_name AND t0.class_name = t3.class_name AND t0.course_name = t3.course_name;


        '''.format(start_time, end_time, Config.SCHOOL_PERFORMANCE_TABLE)

        res = {}
        for row in self.__db.select(sql):
            stu_id = row[2].encode('utf-8')
            if not res.has_key(stu_id):
                res[stu_id] = {}

            course_id = row[3].encode('utf-8')
            if not res[stu_id].has_key(course_id):
                res[stu_id][course_id] = 0
            res[stu_id][course_id] = row[4]

        self.__logger.debug("get_course_grade_level: " + str(res))
        self.__logger.info("Done")
        return res

    def get_course_study_status(self, start_time, end_time):
        ''''''
        self.__logger.info("Try to get all study_status from {0} between {1} and {2}".format(Config.OUTPUT_UI_COURSE_TABLE, start_time, end_time))
        sql = '''
            SELECT
                t.student_number, t.course_name, ROUND(1.0 * (total - {3}) / IF(total <= {3}, {3}, {4} - {3}), 2) AS study_level
            FROM
            (
                SELECT
                    student_number, course_name, COUNT(*) AS total
                FROM {2}
                WHERE dt >= {0} AND dt <= {1} AND student_study_stat != '3'
                GROUP BY student_number, course_name
            ) t
        '''.format(start_time, end_time, Config.OUTPUT_UI_COURSE_TABLE, Config.ANALYSIS_STUDY_STAT_THRESHOLD, Config.ANALYSIS_LOOKBACKWINDOW)

        res = {}
        for row in self.__db.select(sql):
            stu_id = row[0].encode('utf-8')
            if not res.has_key(stu_id):
                res[stu_id] = {}

            course_id = row[1].encode('utf-8')
            if not res[stu_id].has_key(course_id):
                res[stu_id][course_id] = 0
            res[stu_id][course_id] = row[2]

        self.__logger.debug("get_course_study_status: " + str(res))
        self.__logger.info("Done")
        return res