# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('treemenus', '__first__'),
    ]

    operations = [
        migrations.CreateModel(
            name='MenuItemExtension',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('visivel', models.BooleanField(default=False)),
                ('css', models.CharField(max_length=300, null=True, verbose_name='CSS style', blank=True)),
                ('menu_item', models.OneToOneField(related_name='extension', to='treemenus.MenuItem')),
            ],
        ),
    ]