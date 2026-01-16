from rest_framework import serializers
from .models import TrustGuideline


class TrustGuidelineSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrustGuideline
        fields = '__all__'