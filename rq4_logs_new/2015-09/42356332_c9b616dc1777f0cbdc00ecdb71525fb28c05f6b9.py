import os
from sys import path
from django.core import serializers
from django.db import models, migrations
from django.core.management import call_command

fixture_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../fixtures'))
fixture_filename = 'initial_data.json'


def load_fixture(apps, schema_editor):
    call_command('loaddata', 'initial_data', app_label='routebuilder')


def unload_fixture(apps, schema_editor):
    apps.get_model("routebuilder", "Route").objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ('routebuilder', '0003_auto_20150913_0004'),
    ]

    operations = [
        migrations.RunPython(load_fixture, reverse_code=unload_fixture),
    ]