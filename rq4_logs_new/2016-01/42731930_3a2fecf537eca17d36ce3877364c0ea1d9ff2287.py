# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('tracnghiem', '0033_auto_20160118_1133'),
    ]

    operations = [
        migrations.AlterField(
            model_name='baithi',
            name='diem',
            field=models.FloatField(verbose_name=b'\xc4\x90i\xe1\xbb\x83m'),
        ),
    ]