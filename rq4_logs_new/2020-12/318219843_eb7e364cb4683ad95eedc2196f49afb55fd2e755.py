from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from account.models import Account
from rangefilter.filter import DateRangeFilter
from import_export.admin import ImportExportModelAdmin
# Register your models here.


def activate_users(modeladmin, request, queryset):
    queryset.update(is_active=True)


class AccountAdmin(UserAdmin, ImportExportModelAdmin):
    list_display = ('email', 'username', 'atype', 'date_joined',
                    'last_login', 'is_admin', 'is_staff')
    ordering = ['-last_login']
    actions = [activate_users]
    search_fields = ('email', 'username',)
    readonly_fields = ('date_joined', 'last_login')

    filter_horizontal = ()
    list_filter = ('atype', ('last_login', DateRangeFilter),
                   ('date_joined', DateRangeFilter), 'is_active',)
    fieldsets = ()


admin.site.register(Account, AccountAdmin)