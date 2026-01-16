from django import forms
from django.db.models import Q
from django.utils.translation import ugettext_lazy as _

from itertools import chain

from pyston.filters.default_filters import SimpleMethodEqualFilter
from pyston.filters.exceptions import FilterValueError

from is_core.filters import UIFilterMixin

from .enums import CeleryTaskLogState, CeleryTaskRunLogState
from .models import CeleryTaskRunLog


class CeleryTaskLogStateFilter(UIFilterMixin, SimpleMethodEqualFilter):

    EMPTY_CHOICE = ('', '--------')

    widget = forms.Select(choices=chain((EMPTY_CHOICE,), CeleryTaskLogState.choices()))

    def clean_value(self, value, operator_slug, request):
        try:
            return CeleryTaskLogState(int(value))
        except ValueError as ex:
            raise FilterValueError(_('Invalid value.'))

    def get_filter_term(self, value, operator_slug, request):
        if value == CeleryTaskLogState.EXPIRED:
            return Q(is_set_as_stale=True)
        elif value == CeleryTaskLogState.SUCCEEDED:
            return Q(
                celery_task_id__in=CeleryTaskRunLog.objects.filter(
                    state=CeleryTaskRunLogState.SUCCEEDED
                ).values('celery_task_id')
            )
        elif value == CeleryTaskLogState.FAILED:
            return Q(
                celery_task_id__in=CeleryTaskRunLog.objects.filter(
                    state=CeleryTaskRunLogState.FAILED
                ).values('celery_task_id')
            )
        elif value == CeleryTaskLogState.ACTIVE:
            return Q(
                celery_task_id__in=CeleryTaskRunLog.objects.filter(
                    state=CeleryTaskRunLogState.ACTIVE
                ).values('celery_task_id')
            )
        elif value == CeleryTaskLogState.RETRIED:
            return Q(
                celery_task_id__in=CeleryTaskRunLog.objects.filter(
                    state=CeleryTaskRunLogState.RETRIED
                ).values('celery_task_id')
            ) & ~Q(
                celery_task_id__in=CeleryTaskRunLog.objects.filter(
                    state__in={
                        CeleryTaskRunLogState.FAILED, CeleryTaskRunLogState.SUCCEEDED, CeleryTaskRunLogState.ACTIVE
                    }
                ).values('celery_task_id')
            )
        else:
            return ~Q(celery_task_id__in=CeleryTaskRunLog.objects.all().values('celery_task_id'))