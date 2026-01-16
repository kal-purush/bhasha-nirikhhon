from django.http import HttpResponseForbidden
from rest_framework.permissions import BasePermission


class ProviderPermission(BasePermission):

    def has_permission(self, request, view):
        if request.user.groups.filter(name="provider").exists() or request.user.is_staff:
            return True


class DealerPermission(BasePermission):

    def has_permission(self, request, view):
        if request.user.groups.filter(name="dealer").exists() or request.user.is_staff:
            return True


class CustomerPermission(BasePermission):

    def has_permission(self, request, view):
        if request.user.groups.filter(name="customer").exists() or request.user.is_staff:
            return True
