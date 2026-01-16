# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import tagging.fields


class Migration(migrations.Migration):

    dependencies = [
        ('blog', '0005_auto_20170509_2257'),
    ]

    operations = [
        migrations.AddField(
            model_name='blog',
            name='tags',
            field=tagging.fields.TagField(max_length=255, blank=True),
        ),
    ]