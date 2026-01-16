import telegram
from django.contrib import admin
import random

from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.utils.safestring import mark_safe

from bot.handlers.utils  import send_message
from imperiasevastopol_bot.settings import DEBUG
from .forms import BroadcastForm
from .models import *
from bot.tasks import broadcast_message


@admin.register(User)
class UserAdmin(admin.ModelAdmin):

    list_display = [
        'user_id', 'username', 'first_name', 'last_name', 'deep_link', 'language_code',
        'created_at', 'updated_at', 'is_blocked_bot'
    ]
    list_filter = ["is_blocked_bot", "is_moderator"]
    search_fields = ('username', 'user_id')

    actions = ['broadcast']

    def invited_users(self, obj):
        return obj.invited_users().count()

    def broadcast(self, request, queryset):
       #Select users via check mark in django-admin panel, then select "Broadcast" to send message
        if 'apply' in request.POST:
            broadcast_message_text = request.POST["broadcast_text"]

            if len(queryset) <= 3 or DEBUG:  # for test / debug purposes - run in same thread
                for u in queryset:
                    send_message(user_id=u.user_id, text=broadcast_message_text, parse_mode=telegram.ParseMode.MARKDOWN)
                self.message_user(request, "Отправлено %d пользователям" % len(queryset))
            else:
                user_ids = list(set(u.user_id for u in queryset))
                random.shuffle(user_ids)
                broadcast_message.delay(message=broadcast_message_text, user_ids=user_ids)
                self.message_user(request, "Broadcasting of %d messages has been started" % len(queryset))

            return HttpResponseRedirect(request.get_full_path())

        form = BroadcastForm(initial={'_selected_action': queryset.values_list('user_id', flat=True)})
        return render(
            request, "admin/broadcast_message.html", {'items': queryset,'form': form, 'title':u' '}
        )
    broadcast.short_description = 'Рассылка'

@admin.register(UserActionLog)
class UserActionLogAdmin(admin.ModelAdmin):

    list_display = ['user', 'action', 'created_at']


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):

    list_display = ['title', 'slug']
    prepopulated_fields = {'slug': ('title',)}


@admin.register(Post)
class PostAdmin(admin.ModelAdmin):

    list_display = ['title', 'slug', 'category', 'created_at', 'on_top', 'get_photo']
    list_display_links = ['title', 'slug']
    prepopulated_fields = {'slug': ('title',)}

    def get_photo(self, obj):
        if obj.image:
            return mark_safe(f'<img src="{obj.image.url}" width="75">')
        else:
            return '-'
    get_photo.short_description = 'Миниатюра'


@admin.register(Message)
class MessageAdmin(admin.ModelAdmin):
    list_display = ['user', 'text', 'created_at', 'get_photo']

    def get_photo(self, obj):
        if obj.image:
            return mark_safe(f'<img src="{obj.image.url}" width="75">')
        else:
            return '-'
    get_photo.short_description = 'Миниатюра'