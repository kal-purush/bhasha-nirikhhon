# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('polls', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='hashtag',
            old_name='user',
            new_name='hashtag',
        ),
        migrations.RemoveField(
            model_name='hashtag',
            name='settings',
        ),
        migrations.RemoveField(
            model_name='insta',
            name='content',
        ),
        migrations.AlterField(
            model_name='insta',
            name='caption_text',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='content_date',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='content_type',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='image_link',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='lat',
            field=models.DecimalField(null=True, max_digits=20, decimal_places=17, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='link',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='lon',
            field=models.DecimalField(null=True, max_digits=20, decimal_places=17, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='insta',
            name='thumbnail_link',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='socialsetting',
            name='api_access_token',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='socialsetting',
            name='api_client_id',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='socialsetting',
            name='api_last_id',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='team',
            name='van_name',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='content',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='content_date',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='content_id',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='content_type',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='lat',
            field=models.DecimalField(null=True, max_digits=20, decimal_places=17, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='link',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='lon',
            field=models.DecimalField(null=True, max_digits=20, decimal_places=17, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='tweet',
            name='profile_pic',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='verified',
            name='instagram_id',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='verified',
            name='instagram_name',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='verified',
            name='runkeeper_id',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='verified',
            name='runkeeper_name',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='verified',
            name='twitter_id',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='verified',
            name='twitter_name',
            field=models.CharField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
    ]