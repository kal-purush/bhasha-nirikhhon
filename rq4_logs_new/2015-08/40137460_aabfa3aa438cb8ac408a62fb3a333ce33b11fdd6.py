# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('servicemap', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='service',
            name='log_services',
            field=models.ManyToManyField(related_name='log_service', to='servicemap.Service'),
        ),
        migrations.AddField(
            model_name='service',
            name='login_systems',
            field=models.ManyToManyField(related_name='login_service', to='servicemap.Service'),
        ),
        migrations.AlterUniqueTogether(
            name='hostrole',
            unique_together=set([('host', 'role')]),
        ),
    ]