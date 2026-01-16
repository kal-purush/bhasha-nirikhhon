# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('ManageUsers', '0002_auto_20151109_1901'),
    ]

    operations = [
        migrations.AddField(
            model_name='agreement',
            name='date_added',
            field=models.DateField(default=django.utils.timezone.now),
        ),
    ]