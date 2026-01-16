#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import defaultdict


class Activity():

    def __init__(self, user_ip, total_activity, download_activity, good_robot_activity, other_activity):
        self.user_ip = user_ip
        self.total_activity = total_activity
        self.download_activity = download_activity
        self.good_robot_activity = good_robot_activity
        self.other_activity = other_activity
        self.ratio = download_activity * 100.0 / self.total_activity

    def __str__(self):
        return "Activity({:15}, total_activity: {:5}, download_activity: {:5} (ratio: {:4.2f}%), good_robot_activity: {:5}, other_activity: {:5})".format(
            self.user_ip, self.total_activity, self.download_activity,
            self.ratio, self.good_robot_activity, self.other_activity)

    def __repr__(self):
        return str(self)


class ActivityTracker():

    def __init__(self):
        self.total_activity = defaultdict(lambda: 0)
        """{167.2.3.4: 99}"""
        self.download_activity = defaultdict(lambda: 0)
        """{167.2.3.4: 12}"""
        self.good_robot_activity = defaultdict(lambda: 0)
        """{167.2.3.4: 12}"""
        self.other_activity = defaultdict(lambda: 0)
        """{167.2.3.4: 12}"""

    def register_activity(self, record):
        self.total_activity[record.user_ip] += 1

        if record.is_article_download:
            self.download_activity[record.user_ip] += 1

        if record.is_good_robot:
            self.good_robot_activity[record.user_ip] += 1
        else:
            self.other_activity[record.user_ip] += 1

    def get_activities(self):
        results = [Activity(
            user_ip,
            total_activity,
            self.download_activity[user_ip],
            self.good_robot_activity[user_ip],
            self.other_activity[user_ip],
        ) for user_ip, total_activity in self.total_activity.items()]

        return sorted(results, key=lambda activity: -activity.ratio)