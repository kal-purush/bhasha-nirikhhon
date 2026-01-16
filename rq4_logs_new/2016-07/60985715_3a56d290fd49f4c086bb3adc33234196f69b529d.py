# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('collegeSwimming001', '0013_swimmer_gender'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='swimmer',
            name='email',
        ),
        migrations.RemoveField(
            model_name='swimmer',
            name='password_hash',
        ),
        migrations.RemoveField(
            model_name='swimmer',
            name='registration_date',
        ),
        migrations.RemoveField(
            model_name='swimmer',
            name='salt',
        ),
    ]