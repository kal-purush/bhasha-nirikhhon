# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import datetime


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='DontSendEntry',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('to_address', models.EmailField(max_length=254)),
                ('when_added', models.DateTimeField()),
            ],
            options={
                'verbose_name': "don't send entry",
                'verbose_name_plural': "don't send entries",
            },
        ),
        migrations.CreateModel(
            name='Message',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('message_data', models.TextField()),
                ('when_added', models.DateTimeField(default=datetime.datetime.now)),
                ('priority', models.CharField(default=b'2', max_length=1, choices=[(b'1', b'high'), (b'2', b'medium'), (b'3', b'low'), (b'4', b'deferred')])),
            ],
        ),
        migrations.CreateModel(
            name='MessageLog',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('message_data', models.TextField()),
                ('when_added', models.DateTimeField()),
                ('priority', models.CharField(max_length=1, choices=[(b'1', b'high'), (b'2', b'medium'), (b'3', b'low'), (b'4', b'deferred')])),
                ('when_attempted', models.DateTimeField(default=datetime.datetime.now, db_index=True)),
                ('result', models.CharField(max_length=1, choices=[(b'1', b'success'), (b'2', b"don't send"), (b'3', b'failure')])),
                ('log_message', models.TextField()),
            ],
        ),
    ]