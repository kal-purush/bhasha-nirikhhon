from django.shortcuts import render, get_object_or_404
from django_hosts import reverse

from core.Utils.Access.decorators import manager_required
from core.User.models import User
from .forms import UsersFilterForm
from .tables import UsersTable


def get_base_qs():
    return User.objects.users().order_by('email')


@manager_required
def users_list(request):
    users = get_base_qs()

    user_filter = UsersFilterForm(request.GET, queryset=users)
    users = user_filter.qs
    table_body = UsersTable(users, request=request)

    table = {
        'body': table_body,
        'filter': {
            'body': user_filter,
            'action': reverse('admin-users-list', host='admin'),
        }
    }

    return render(request, 'Admin/Users/users_list.html',
                  {'table': table})


@manager_required
def users_view(request, user_pk):
    admin = get_object_or_404(get_base_qs(), pk=user_pk)
    return render(request, 'Admin/Users/users_view.html', {'admin': admin})