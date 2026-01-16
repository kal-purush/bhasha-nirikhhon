from django.contrib import admin, messages
from django.utils.translation import ngettext

from datetime import date
from APIs.Util.CommonFunc import CreatePost


@admin.action(description='Mark selected Submission as published')
def make_published(self, request, queryset):
    # print(request)
    CreatePost(queryset, request)
    updated = queryset.update(
        Status=2, ApprovedOn=date.today(), ApprovedBy=request.user)
    self.message_user(request, ngettext(
        '%d story was successfully marked as published.',
        '%d stories were successfully marked as published.',
        updated,
    ) % updated, messages.SUCCESS)


@admin.action(description='Mark selected Submission as Submitted')
def make_submitted(self, request, queryset):
    # print(request)
    updated = queryset.update(
        Status=0, ApprovedOn=None, ApprovedBy=None)
    self.message_user(request, ngettext(
        '%d story was successfully marked as Submitted.',
        '%d stories were successfully marked as Submitted.',
        updated,
    ) % updated, messages.SUCCESS)