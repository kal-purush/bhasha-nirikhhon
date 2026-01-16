# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('devices', '0019_auto_20150916_2305'),
    ]

    operations = [
        migrations.CreateModel(
            name='Organization',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)])),
                ('created', models.DateTimeField(auto_now_add=True, verbose_name=b'created')),
                ('last_modified', models.DateTimeField(auto_now=True, verbose_name=b'last modified')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.AlterField(
            model_name='actuator',
            name='name',
            field=models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)]),
        ),
        migrations.AlterField(
            model_name='actuatortype',
            name='name',
            field=models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)]),
        ),
        migrations.AlterField(
            model_name='network',
            name='name',
            field=models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)]),
        ),
        migrations.AlterField(
            model_name='node',
            name='name',
            field=models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)]),
        ),
        migrations.AlterField(
            model_name='sensor',
            name='name',
            field=models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)]),
        ),
        migrations.AlterField(
            model_name='sensortype',
            name='name',
            field=models.SlugField(max_length=45, validators=[django.core.validators.MinLengthValidator(1)]),
        ),
        migrations.AddField(
            model_name='network',
            name='owner',
            field=models.ForeignKey(default=1, to='devices.Organization'),
            preserve_default=False,
        ),
    ]