from rest_framework import permissions


class AuthenticatedOrReadOnly(permissions.BasePermission):
    def has_permission(self, request, view):
        return (
            request.method in permissions.SAFE_METHODS
            or request.user.is_authenticated
        )

    def has_object_permission(self, request, view, obj):
        return view.action == 'retrieve' or request.user.is_authenticated


class AuthorOrCreateOrReadOnly(permissions.BasePermission):
    def has_permission(self, request, view):
        return True

    def has_object_permission(self, request, view, obj):
        return view.action == 'retrieve' or (request.user.is_authenticated and
                                             obj.email == request.user.email)