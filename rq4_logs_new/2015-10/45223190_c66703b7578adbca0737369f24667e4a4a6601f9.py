# -*- coding: utf-8 -*-
"""documentation"""
from __future__ import unicode_literals
import datetime

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models


class Topic(models.Model):
    """docs"""
    id = models.AutoField(primary_key=True)
    subject = models.CharField(max_length=50)
    description = models.TextField()
    DEPTH_BEGINNER = 'Beginner'
    DEPTH_INTERMEDIATE = 'Intermediate'
    DEPTH_EXPERT = 'Expert'
    DEPTH_CHOICES = (
        ('B', DEPTH_BEGINNER),
        ('I', DEPTH_INTERMEDIATE),
        ('E', DEPTH_EXPERT),
    )
    depth = models.CharField(max_length=1, choices=DEPTH_CHOICES)
    suggested_by = models.ForeignKey(User)
    suggested_date = models.DateField(default=datetime.date.today)

    def __unicode__(self):
        """useful for debugging..  actually see what the instance is"""
        return self.subject


class User(models.Model):
    """best practices multi-line doc

    Additional doc
    """
    id = models.AutoField(primary_key=True)
    photo = models.ImageField()
    email = models.CharField(max_length=100, unique=True)
    first_name = models.CharField(max_length=20)
    last_name = models.CharField(max_length=20)
    topic_interest = models.ManyToManyField(Topic,
                                            related_name='user_interest',
                                            through='Interest')
    topic_skill_level = models.ManyToManyField(
        Topic, related_name='user_skill_level', through='SkillLevel')

    def __unicode__(self):
        return self.full_name

    @property
    def full_name(self):
        return "{} {}".format(self.first_name, self.last_name)


class Interest(models.Model):
    """doc"""
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User)
    topic = models.ForeignKey(Topic)
    level = models.PositiveSmallIntegerField(null=True)

    def __unicode__(self):
        return "{}'s interest in {}".format(self.user, self.topic)


class SkillLevel(models.Model):
    """doc"""
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User)
    topic = models.ForeignKey(Topic)
    level = models.PositiveSmallIntegerField(null=True)

    def __unicode__(self):
        return "{} aptitude of {} is {}".format(
            self.user, self.topic, self.level)


class Presentation(models.Model):
    id = models.AutoField(primary_key=True)
    topic = models.ForeignKey(Topic)
    presenter = models.ForeignKey(User)
    when = models.DateField()
    feedback = models.ManyToManyField(User, through='Feedback')


class Feedback(models.Model):
    id = models.AutoField(primary_key=True)
    prep_level = models.PositiveSmallIntegerField(
        default=1,
        validators=[
            MaxValueValidator(9),
            MinValueValidator(1)
        ]
    )
    comments = models.TextField()