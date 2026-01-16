# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Fact',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, auto_created=True, verbose_name='ID')),
                ('fact', models.CharField(max_length=256)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Fact_Type',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, auto_created=True, verbose_name='ID')),
                ('sortorder', models.PositiveIntegerField(unique=True)),
                ('public', models.BooleanField(default=False)),
                ('name', models.CharField(max_length=256)),
                ('prompt_type', models.SmallIntegerField(choices=[(0, 'text'), (1, 'blob')])),
                ('question', models.TextField()),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Name',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, auto_created=True, verbose_name='ID')),
                ('name', models.CharField(max_length=256)),
                ('correct', models.BooleanField(default=False)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Opinion',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, auto_created=True, verbose_name='ID')),
                ('opinion', models.TextField()),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Opinion_Prompt',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, auto_created=True, verbose_name='ID')),
                ('sortorder', models.PositiveIntegerField(unique=True)),
                ('prompt_text', models.TextField()),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Photo',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, auto_created=True, verbose_name='ID')),
                ('url', models.TextField()),
                ('primary', models.BooleanField(default=False)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Prospective',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='photo',
            name='prospective_id',
            field=models.ForeignKey(to='nameslist_app.Prospective'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='photo',
            name='user_id',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='photo',
            unique_together=set([('prospective_id', 'user_id')]),
        ),
        migrations.AddField(
            model_name='opinion',
            name='opinion_prompt_id',
            field=models.ForeignKey(to='nameslist_app.Opinion_Prompt'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='opinion',
            name='prospective_id',
            field=models.ForeignKey(to='nameslist_app.Prospective'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='opinion',
            name='user_id',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='opinion',
            unique_together=set([('opinion_prompt_id', 'user_id', 'prospective_id')]),
        ),
        migrations.AddField(
            model_name='name',
            name='prospective_id',
            field=models.ForeignKey(to='nameslist_app.Prospective'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='name',
            name='user_id',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='name',
            unique_together=set([('prospective_id', 'user_id')]),
        ),
        migrations.AddField(
            model_name='fact',
            name='fact_type_id',
            field=models.ForeignKey(to='nameslist_app.Fact_Type'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fact',
            name='prospective_id',
            field=models.ForeignKey(to='nameslist_app.Prospective'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='fact',
            name='user_id',
            field=models.ForeignKey(null=True, to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='fact',
            unique_together=set([('fact_type_id', 'user_id', 'prospective_id')]),
        ),
    ]