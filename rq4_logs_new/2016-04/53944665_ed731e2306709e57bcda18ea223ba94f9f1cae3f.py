from rest_framework import serializers
from .models import Comment


class CommentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Comment
        depth = 2
        fields = ('pk',
                  'date',
                  'text',
                  'from_user',
                  'to_user',
                  'category')