"""
© Ocado Group
Created on 20/01/2025 at 16:26:47(+00:00).
"""

from codeforlife.filters import FilterSet  # isort: skip
from codeforlife.user.models import AuthFactor  # isort: skip
from django_filters import (  # type: ignore[import-untyped] # isort: skip
    rest_framework as filters,
)


# pylint: disable-next=missing-class-docstring
class AuthFactorFilterSet(FilterSet):
    user = filters.NumberFilter("user")
    type = filters.ChoiceFilter(choices=AuthFactor.Type.choices)

    class Meta:
        model = AuthFactor
        fields = ["user", "type"]