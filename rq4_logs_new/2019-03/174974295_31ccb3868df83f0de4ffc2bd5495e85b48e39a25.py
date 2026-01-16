# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .models import SpotPost, StreetPost
from django.contrib import admin

# Register your models here.
#admin.site.register(UserProfile)
@admin.register(SpotPost)
@admin.register(StreetPost)

class SpotPostAdmin(admin.ModelAdmin):
	list_display = ['id', 'title', 'created_at']

	def __str__(self):
		return self.title

class StreetPostAdmin(admin.ModelAdmin):
	list_display = ['id', 'title', 'created_at']
	filter_horizontal = ('spots',)
	search_fields = ['title']
	
	def __str__(self):
		return self.title