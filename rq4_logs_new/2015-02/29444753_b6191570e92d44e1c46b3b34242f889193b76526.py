# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import datetime


class Migration(migrations.Migration):

    dependencies = [
        ('webms', '0006_auto_20150131_0012'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='webm',
            options={'ordering': ['-updated']},
        ),
        migrations.AddField(
            model_name='webm',
            name='hits',
            field=models.IntegerField(default=1),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='webm',
            name='updated',
            field=models.DateTimeField(default=datetime.datetime(2015, 2, 11, 21, 24, 6, 391509), auto_now=True),
            preserve_default=True,
        ),
    ]