"""
© Ocado Group
Created on 20/02/2025 at 15:23:05(+00:00).
"""

from codeforlife.serializers import ModelSerializer
from codeforlife.user.models import OtpBypassToken, User

# pylint: disable=missing-class-docstring
# pylint: disable=too-many-ancestors


class OtpBypassTokenSerializer(ModelSerializer[User, OtpBypassToken]):
    class Meta:
        model = OtpBypassToken
        fields = ["decrypted_token"]
        extra_kwargs = {"decrypted_token": {"read_only": True}}