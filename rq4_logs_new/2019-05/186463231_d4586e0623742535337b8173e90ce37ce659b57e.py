from django.contrib import admin

from accounts.models import MockUser, MockProfile


@admin.register(MockUser)
class MockUserAdmin(admin.ModelAdmin):
    list_display = ('email', 'username', 'created', 'modified', 'is_superuser',)
    fields = ('email', 'username', 'is_staff', 'is_superuser', )


@admin.register(MockProfile)
class MockProfileAdmin(admin.ModelAdmin):
    list_display = ('nickname', 'created', 'modified', 'language', 'country', 'locale',)
    fields = ('nickname', 'language', 'country', 'locale', )