from rest_framework import serializers

from src.auth.auth_models.identifying_document import IdentifyingDocument

class IDSerializer(serializers.ModelSerializer):
    user = serializers.HiddenField(
        default=serializers.CurrentUserDefault()
    )

    def validate(self, data):
        if data['identification_type'] != 'tz':
            return data
        else:
            #TODO: validate Israeli TZ
            return data

    class Meta:
        model = IdentifyingDocument
        fields = ('id', 'identification_number', 'identification_type', 'passport_country', 'user')