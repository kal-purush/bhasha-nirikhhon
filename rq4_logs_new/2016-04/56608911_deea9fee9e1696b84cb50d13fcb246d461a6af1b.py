# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Resource',
            fields=[
                ('id', models.AutoField(verbose_name='ID',
                                        serialize=False,
                                        auto_created=True,
                                        primary_key=True)),
                ('path', models.CharField(unique=True, max_length=128)),
                ('content', models.TextField(blank=True)),
                ('content_type', models.CharField(max_length=128, blank=True)),
            ],
            options={
                'ordering': ('path',),
            },
            bases=(models.Model,),
        ),
    ]