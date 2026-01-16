# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('ManageIdea', '0008_auto_20151107_2156'),
    ]

    operations = [
        migrations.CreateModel(
            name='Steckbrief',
            fields=[
                ('id', models.AutoField(serialize=False, verbose_name='ID', auto_created=True, primary_key=True)),
                ('date_added', models.DateField(default=django.utils.timezone.now)),
            ],
        ),
        migrations.AddField(
            model_name='idea',
            name='secret',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='idea',
            name='description_long',
            field=models.CharField(max_length=2048),
        ),
        migrations.AlterField(
            model_name='idea',
            name='description_short',
            field=models.CharField(max_length=2048),
        ),
        migrations.AlterField(
            model_name='ideamembership',
            name='task',
            field=models.CharField(max_length=64, null=True, blank=True),
        ),
        migrations.AddField(
            model_name='steckbrief',
            name='idea',
            field=models.ForeignKey(to='ManageIdea.Idea'),
        ),
    ]