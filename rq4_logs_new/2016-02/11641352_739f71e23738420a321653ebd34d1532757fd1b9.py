# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import cms.models.fields


class Migration(migrations.Migration):

    dependencies = [
        ('cms', '0013_urlconfrevision'),
    ]

    operations = [
        migrations.CreateModel(
            name='LinkBlock',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('title', models.CharField(unique=True, max_length=255, verbose_name='menu title', db_index=True)),
                ('links', cms.models.fields.PlaceholderField(slotname=b'cmsplugin_menus link block', editable=False, to='cms.Placeholder', help_text='Add links, e-mails, snippets to the menu.', null=True)),
            ],
            options={
                'ordering': ['title'],
                'verbose_name': 'Custom Menu',
                'verbose_name_plural': 'Custom Menus',
            },
        ),
        migrations.CreateModel(
            name='LinkBlockPtr',
            fields=[
                ('cmsplugin_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='cms.CMSPlugin')),
                ('collapse', models.BooleanField(default=False, help_text=b'Select this option if the menu should initially appear collapsed.', verbose_name='collapse menu')),
                ('link_block', models.ForeignKey(to='linkblock.LinkBlock')),
            ],
            options={
                'verbose_name': 'Custom Menu Plugin Model',
            },
            bases=('cms.cmsplugin',),
        ),
    ]