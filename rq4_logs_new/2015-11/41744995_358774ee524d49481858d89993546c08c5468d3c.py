# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('restaurants', '0010_menuitemtype_is_base_price'),
        ('orders', '0003_order_restaurant'),
    ]

    operations = [
        migrations.CreateModel(
            name='OrderMenuItemTypeOption',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, verbose_name='ID', auto_created=True)),
                ('menu_item_type_option', models.ForeignKey(to='restaurants.MenuItemTypeOption')),
                ('order', models.ForeignKey(to='orders.Order')),
            ],
        ),
    ]