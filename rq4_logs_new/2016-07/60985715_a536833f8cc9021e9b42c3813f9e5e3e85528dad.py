# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('collegeSwimming001', '0016_auto_20160704_1140'),
    ]

    operations = [
        migrations.RenameField(
            model_name='meet',
            old_name='endDate',
            new_name='end_date',
        ),
        migrations.RenameField(
            model_name='meet',
            old_name='startDate',
            new_name='start_date',
        ),
    ]