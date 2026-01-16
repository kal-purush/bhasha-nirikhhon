# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('polls', '0002_auto_20150815_1453'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='insta',
            name='hashtag',
        ),
        migrations.RemoveField(
            model_name='insta',
            name='verified',
        ),
        migrations.RemoveField(
            model_name='tweet',
            name='hashtag',
        ),
        migrations.RemoveField(
            model_name='tweet',
            name='verified',
        ),
        migrations.AddField(
            model_name='hashtag',
            name='instas',
            field=models.ForeignKey(to='polls.Insta', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='hashtag',
            name='tweets',
            field=models.ForeignKey(to='polls.Tweet', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='insta',
            name='known_user',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='tweet',
            name='known_user',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='verified',
            name='instas',
            field=models.ForeignKey(to='polls.Insta', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='verified',
            name='tweets',
            field=models.ForeignKey(to='polls.Tweet', null=True),
            preserve_default=True,
        ),
    ]