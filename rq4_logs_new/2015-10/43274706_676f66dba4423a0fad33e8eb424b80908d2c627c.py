# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('mapa', '0002_auto_20151006_2223'),
    ]

    operations = [
        migrations.AlterIndexTogether(
            name='mort',
            index_together=set([('year', 'country', 'admin1', 'cause', 'sex')]),
        ),
    ]