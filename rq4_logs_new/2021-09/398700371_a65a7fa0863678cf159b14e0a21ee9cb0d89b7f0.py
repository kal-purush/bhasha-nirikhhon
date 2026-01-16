from django.contrib.admin.decorators import action
from blog.admin import CommentInline, PostInline
from .models import Profile
from django.contrib import admin
# Register your models here.


@admin.register(Profile)
class ProfileAdmin(admin.ModelAdmin):
    list_display = ['get_full_name','mobile_number','national_code','gender','is_auther','get_create_date']
    list_display_links =['get_full_name']
    list_filter = ['create_date','gender','is_auther']
    search_fields = ['user__username','user__first_name','user__last_name','mobile_number','national_code']
    sortable_by = ['get_full_name','get_create_date']
    ordering = ['user__last_name','user__first_name','age']
    list_editable = ['mobile_number','national_code','gender','is_auther']
    inlines = [PostInline, CommentInline]


    
    def get_full_name(self, obj):
        try:
            return "%s %s" %(obj.user.first_name, obj.user.last_name)
        except:
            return "---"

    def get_create_date(self,obj):
        return obj.create_date.strftime('%H:%m - %Y/%m/%d')
    get_create_date.short_description = 'تاریخ ایجاد'

    get_full_name.admin_order_field = 'user__last_name'  
    get_create_date.admin_order_field = 'create_date'  
    get_full_name.short_description = 'نام و نام خانوادگی'