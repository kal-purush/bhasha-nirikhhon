from django.contrib import admin

from account import models


@admin.register(models.OAuthToken)
class OAuthTokenAdmin(admin.ModelAdmin):
    list_display = 'id', 'user', 'service', 'auth_token_expiration'
    list_display_links = 'id',
    search_fields = 'user',
    list_filter = 'service',
    date_hierarchy = 'auth_token_expiration'


@admin.register(models.OAuthService)
class OAuthServiceAdmin(admin.ModelAdmin):
    list_display = 'display_name', 'name', 'enabled'
    list_display_links = 'display_name',
    search_fields = 'display_name', 'name'
    list_filter = 'enabled', 'name'