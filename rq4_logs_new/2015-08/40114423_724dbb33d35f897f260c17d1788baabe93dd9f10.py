# -*- coding: utf-8 -*-
from django.contrib import admin
from .models import DocCategory, DocManager, Doc
from .plugin import DocPlugin


class DocCategoryAdmin(admin.ModelAdmin):
    prepopulated_fields = {'slug': ('title', )}

admin.site.register(DocCategory, DocCategoryAdmin)
admin.site.register(DocManager, DocPlugin)
admin.site.register(Doc)