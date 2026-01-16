# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('propapp', '0002_auto_20151119_1810'),
    ]

    operations = [
        migrations.CreateModel(
            name='General_Options',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, verbose_name='ID', serialize=False)),
                ('front_title', models.CharField(max_length=15)),
            ],
        ),
        migrations.CreateModel(
            name='Prop_Images',
            fields=[
                ('name', models.CharField(max_length=10)),
                ('image_id', models.AutoField(primary_key=True, default=1, serialize=False)),
                ('image_path', models.CharField(max_length=20)),
                ('image_thumb', models.CharField(max_length=20)),
                ('property_id', models.ForeignKey(to='propapp.Property')),
            ],
        ),
    ]