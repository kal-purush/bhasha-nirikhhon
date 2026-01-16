from django.contrib import admin

# 这个admin是django自己的界面
# Register your models here.

from tweets.models import Tweet

@admin.register(Tweet)
class TweetAdmin(admin.ModelAdmin):
    date_hierarchy = 'created_at'
    # 把这几项像表格一样列出来，白名单模式
    list_display = (
        'created_at',
        'user',
        'content',
    )