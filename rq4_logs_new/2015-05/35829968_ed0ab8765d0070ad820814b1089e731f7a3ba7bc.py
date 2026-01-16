# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import datetime
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Hobby',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=128)),
            ],
        ),
        migrations.CreateModel(
            name='UserProfile',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('birthdate', models.DateField(blank=True)),
                ('hair_color', models.CharField(blank=True, max_length=128, choices=[(b'BLACK', b'Black'), (b'BROWN', b'Brown'), (b'RED', b'Red'), (b'BLONDE', b'Blonde'), (b'SALTNPEPPER', b'Salt N Peppa'), (b'GREEN', b'Green')])),
                ('created', models.DateTimeField(default=datetime.datetime(2015, 5, 18, 18, 32, 47, 413437))),
                ('favorite_hobby', models.OneToOneField(to='advanced_tactics.Hobby')),
                ('user', models.OneToOneField(to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]