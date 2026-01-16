from rest_framework import serializers
from ..models import Topics

# Serializers define the API representation.
class TopicsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Topics
        fields = ['id', 'topicname', 'context', 'example']