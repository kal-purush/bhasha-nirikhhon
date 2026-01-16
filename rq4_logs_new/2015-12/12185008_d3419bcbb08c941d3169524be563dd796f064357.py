# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('qsaui', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='episode',
            name='tvdb_id',
            field=models.CharField(max_length=256),
        ),
        migrations.AlterField(
            model_name='series',
            name='tvdb_id',
            field=models.CharField(max_length=256),
        ),
    ]