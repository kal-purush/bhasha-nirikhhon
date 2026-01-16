# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Story',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('title', models.CharField(max_length=200)),
                ('category', models.IntegerField()),
                ('body', models.CharField(max_length=200000)),
                ('uuid', models.CharField(null=True, editable=False, max_length=32, blank=True, unique=True, db_index=True)),
            ],
        ),
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('surname', models.CharField(max_length=30)),
                ('username', models.CharField(max_length=30)),
                ('password', models.CharField(max_length=20)),
                ('uuid', models.CharField(null=True, editable=False, max_length=32, blank=True, unique=True, db_index=True)),
            ],
        ),
        migrations.AddField(
            model_name='story',
            name='author',
            field=models.ForeignKey(to='backend.User'),
        ),
    ]