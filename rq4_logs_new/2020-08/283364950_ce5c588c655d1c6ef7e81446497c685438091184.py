
from rest_framework import serializers

from companies.models import Company, BranchCompany, BranchServices, Qualification


class CompanySerializer(serializers.ModelSerializer):
    class Meta:
        model = Company
        exclude = (
            "logo", "description", "site", "nit",
        )

class BranchCompanySerializer(serializers.ModelSerializer):
    class Meta:
        model = BranchCompany
        exclude = (
            "geolocation", "schedule",
        )

class BranchServicesSerializer(serializers.ModelSerializer):
    class Meta:
        model = BranchServices
        fields = "__all__"

class QualificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Qualification
        exclude = (
            "comment",
        )