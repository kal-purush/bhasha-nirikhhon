from rest_framework import serializers

from news.models import News, Comments


class NewsSerializer(serializers.ModelSerializer):
    """
    Сериализатор для новостей.
    """

    class Meta:
        model = News
        fields = (
            'pk',
            'title',
            'text',
            'image',
            'pub_date',
            'slug',
            'meta_title',
            'meta_description',
        )


class CommentsSerializer(serializers.ModelSerializer):
    """
    Сериализатор для комментариев.
    """

    class Meta:
        model = Comments
        fields = (
            'pk',
            'author',
            'news',
            'comment',
            'pub_date',
        )