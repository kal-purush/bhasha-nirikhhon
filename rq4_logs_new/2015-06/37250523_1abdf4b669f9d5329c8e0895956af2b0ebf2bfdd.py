# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='HouseSales',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('transaction_id', models.CharField(max_length=40)),
                ('price', models.DecimalField(max_digits=12, decimal_places=2)),
                ('date_of_transfer', models.DateField()),
                ('postcode', models.CharField(max_length=10)),
                ('property_type', models.CharField(max_length=1)),
                ('old_new', models.CharField(max_length=1)),
                ('duration', models.CharField(max_length=1)),
                ('paon', models.CharField(max_length=250)),
                ('saon', models.CharField(max_length=250)),
                ('street', models.CharField(max_length=250)),
                ('locality', models.CharField(max_length=250)),
                ('town_city', models.CharField(max_length=250)),
                ('district', models.CharField(max_length=250)),
                ('county', models.CharField(max_length=250)),
                ('status', models.CharField(max_length=1)),
            ],
        ),
    ]