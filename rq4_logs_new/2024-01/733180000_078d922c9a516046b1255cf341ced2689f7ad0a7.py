from django.shortcuts import render
from django_hosts import reverse

from core.Utils.logger import ActivityLog, LevelChoices, log_qs_to_dict
from core.Utils.Access.decorators import manager_required
# from .forms import LoggerFilterForm
from .tables import LoggerTable, LoggerObjectTable


@manager_required
def activity_log_objects_list(request):
    qs = ActivityLog.objects.filter(level=LevelChoices.OBJECT).order_by('-stamp')

    # activity_log_filter = LoggerFilterForm(request.GET, queryset=qs)
    # qs = activity_log_filter.qs
    qs = []
    table_body = LoggerObjectTable(qs)

    table = {
        'body': table_body,
        # 'filter': {
        #     'body': activity_log_filter,
        #     'action': reverse('admin-logger-objects-list', host='admin'),
        # }
    }

    return render(request, 'Admin/Logger/activity_log_objects_list.html',
                  {'table': table})


@manager_required
def activity_log_list(request):
    qs = ActivityLog.objects.all().order_by('-stamp')

    # activity_log_filter = LoggerFilterForm(request.GET, queryset=qs)
    # qs = activity_log_filter.qs
    # data = log_qs_to_dict(qs, fields=LoggerTable.Meta.fields)
    data = []
    table_body = LoggerTable(data)

    table = {
        'body': table_body,
        # 'filter': {
        #     'body': activity_log_filter,
        #     'action': reverse('admin-logger-list', host='admin'),
        # }
    }

    return render(request, 'Admin/Logger/activity_log_list.html',
                  {'table': table})