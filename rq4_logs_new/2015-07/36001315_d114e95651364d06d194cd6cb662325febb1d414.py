# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('farmaciapp', '0035_auto_20150701_1121'),
    ]

    operations = [
        migrations.AddField(
            model_name='pazientedispentsazio',
            name='identBi',
            field=models.AutoField(default=1, primary_key=True),
            preserve_default=False,
        ),
    ]