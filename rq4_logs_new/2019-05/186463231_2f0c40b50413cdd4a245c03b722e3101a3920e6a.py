from django_countries.serializer_fields import CountryField
from rest_framework import serializers
from rest_framework_serializer_extensions.serializers import SerializerExtensionsMixin

from accounts.models import MockUser, MockProfile


class MockUserSerializer(SerializerExtensionsMixin, serializers.ModelSerializer):
    language = serializers.CharField(write_only=True, max_length=2)
    country = CountryField(write_only=True)
    locale = serializers.CharField(write_only=True, max_length=5)

    def create(self, validated_data):
        # validated_data: email, password, language, country, locale
        return MockUser.create(**validated_data)

    class Meta:
        model = MockUser
        fields = ('email', 'password', 'locale', 'country', 'language', )
        write_only_fields = ('email', 'password', )


class MockProfileSerializer(SerializerExtensionsMixin, serializers.ModelSerializer):
    class Meta:
        model = MockProfile
        fields = ('nickname', 'locale', 'country', 'language', )
        read_only_fields = ('nickname', 'locale', 'country', 'language', )