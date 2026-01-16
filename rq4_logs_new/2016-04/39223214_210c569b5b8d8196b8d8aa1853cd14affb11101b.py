# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import mdot_rest.optimizedpngfield


class Migration(migrations.Migration):

    dependencies = [
        ('mdot_rest', '0008_auto_20150730_2217'),
    ]

    operations = [
        migrations.AlterField(
            model_name='uwresource',
            name='image',
            field=mdot_rest.optimizedpngfield.OptimizedPNGImageField(null=True, upload_to=b'uploads', blank=True),
        ),
    ]