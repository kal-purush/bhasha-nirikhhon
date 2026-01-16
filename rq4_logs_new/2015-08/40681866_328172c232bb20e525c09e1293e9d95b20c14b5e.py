# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Hashtag',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('user', models.CharField(max_length=200)),
                ('tweet_count', models.IntegerField(default=0)),
                ('insta_count', models.IntegerField(default=0)),
                ('verified_count', models.IntegerField(default=0)),
                ('unverified_count', models.IntegerField(default=0)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Insta',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('user_name', models.CharField(max_length=200)),
                ('content', models.CharField(max_length=200)),
                ('caption_text', models.CharField(max_length=200)),
                ('content_type', models.CharField(max_length=200)),
                ('thumbnail_link', models.CharField(max_length=200)),
                ('image_link', models.CharField(max_length=200)),
                ('lat', models.DecimalField(max_digits=20, decimal_places=17)),
                ('lon', models.DecimalField(max_digits=20, decimal_places=17)),
                ('link', models.CharField(max_length=200)),
                ('content_date', models.CharField(max_length=200)),
                ('hashtag', models.ForeignKey(to='polls.Hashtag')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='SocialSetting',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('api_type', models.CharField(default=b'TW', max_length=2, choices=[(b'TW', b'Twitter'), (b'IN', b'Instagram')])),
                ('api_name', models.CharField(max_length=200)),
                ('api_client_id', models.CharField(max_length=200)),
                ('api_client_secret', models.CharField(max_length=200)),
                ('api_access_token', models.CharField(max_length=200)),
                ('api_last_id', models.CharField(max_length=200)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Team',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('team_name', models.CharField(max_length=200)),
                ('van_name', models.CharField(max_length=200)),
                ('tweet_count', models.IntegerField(default=0)),
                ('insta_count', models.IntegerField(default=0)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Tweet',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('user_name', models.CharField(max_length=200)),
                ('content', models.CharField(max_length=200)),
                ('content_id', models.CharField(max_length=200)),
                ('content_type', models.CharField(max_length=200)),
                ('lat', models.DecimalField(max_digits=20, decimal_places=17)),
                ('lon', models.DecimalField(max_digits=20, decimal_places=17)),
                ('link', models.CharField(max_length=200)),
                ('profile_pic', models.CharField(max_length=200)),
                ('content_date', models.CharField(max_length=200)),
                ('hashtag', models.ForeignKey(to='polls.Hashtag')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Verified',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('display_name', models.CharField(max_length=200)),
                ('twitter_name', models.CharField(max_length=200)),
                ('twitter_id', models.CharField(max_length=200)),
                ('instagram_name', models.CharField(max_length=200)),
                ('instagram_id', models.CharField(max_length=200)),
                ('runkeeper_name', models.CharField(max_length=200)),
                ('runkeeper_id', models.CharField(max_length=200)),
                ('tweet_count', models.IntegerField(default=0)),
                ('insta_count', models.IntegerField(default=0)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='tweet',
            name='verified',
            field=models.ForeignKey(to='polls.Verified'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='team',
            name='members',
            field=models.ForeignKey(to='polls.Verified'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='insta',
            name='verified',
            field=models.ForeignKey(to='polls.Verified'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='hashtag',
            name='settings',
            field=models.ForeignKey(to='polls.SocialSetting'),
            preserve_default=True,
        ),
    ]