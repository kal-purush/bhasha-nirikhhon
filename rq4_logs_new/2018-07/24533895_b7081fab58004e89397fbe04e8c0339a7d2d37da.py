# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('zues', '0003_add_onderwerp'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='categorie',
            options={'verbose_name_plural': 'categorieën'},
        ),
        migrations.AlterModelOptions(
            name='organimo',
            options={'verbose_name_plural': "organimo's", 'ordering': ('-laatsteupdate',)},
        ),
    ]