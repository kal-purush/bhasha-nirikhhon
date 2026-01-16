# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from .models import Popular, Collaborator

admin.site.register(Popular)
admin.site.register(Collaborator)