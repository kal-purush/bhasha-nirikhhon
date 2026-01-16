# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('blog', '0008_post_alquiler'),
    ]

    operations = [
        migrations.RenameField(
            model_name='post',
            old_name='title',
            new_name='titulo',
        ),
        migrations.RemoveField(
            model_name='post',
            name='text',
        ),
        migrations.AddField(
            model_name='post',
            name='archivo',
            field=models.FileField(default=1, upload_to=b'documents/'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='post',
            name='texto',
            field=models.TextField(default=1),
            preserve_default=False,
        ),
    ]