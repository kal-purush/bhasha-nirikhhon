# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('financeiro', '0003_auto_20150924_1055'),
    ]

    operations = [
        migrations.AddField(
            model_name='extratofinanceiro',
            name='entrada_extrato_cc',
            field=models.ForeignKey(on_delete=django.db.models.deletion.SET_NULL, blank=True, to='financeiro.ExtratoCC', null=True),
        ),
    ]