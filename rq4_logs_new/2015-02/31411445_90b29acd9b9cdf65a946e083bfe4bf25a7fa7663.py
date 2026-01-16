# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('contenttypes', '0001_initial'),
        ('default', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ObjectTokenSocialAuth',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('object_id', models.PositiveIntegerField(null=True, blank=True)),
                ('token', models.CharField(max_length=32)),
                ('content_type', models.ForeignKey(blank=True, to='contenttypes.ContentType', null=True)),
            ],
            options={
                'db_table': 'social_auth_object_token',
            },
            bases=(models.Model,),
        ),
        migrations.RemoveField(
            model_name='usersocialauth',
            name='user',
        ),
        migrations.AddField(
            model_name='usersocialauth',
            name='content_type',
            field=models.ForeignKey(blank=True, to='contenttypes.ContentType', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='usersocialauth',
            name='object_id',
            field=models.PositiveIntegerField(null=True, blank=True),
            preserve_default=True,
        ),
    ]