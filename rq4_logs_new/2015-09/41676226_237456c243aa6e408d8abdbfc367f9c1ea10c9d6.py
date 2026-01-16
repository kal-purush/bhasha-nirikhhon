# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fires', '0001_initial'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='fire',
            unique_together=set([('date', 'satellite', 'confidence', 'frp', 'brightness21', 'brightness31', 'scan', 'track', 'version', 'geometry')]),
        ),
    ]