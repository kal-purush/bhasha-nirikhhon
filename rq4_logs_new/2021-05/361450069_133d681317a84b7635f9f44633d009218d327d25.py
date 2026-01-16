from django.db import models

# Create your models here.
from django.utils.text import Truncator

from authentication.models import CustomUser


class BoardModel(models.Model):
    name = models.CharField(max_length=30, unique=True)
    description = models.CharField(max_length=100)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'board'
        verbose_name = 'boards'


class TopicModel(models.Model):
    subject = models.CharField(max_length=255)
    last_update = models.DateTimeField(auto_now_add=True)
    board = models.ForeignKey(related_name='topics', on_delete=models.CASCADE)
    created_id = models.ForeignKey(CustomUser, related_name='topics', on_delete=models.CASCADE)
    last_updated = models.DateTimeField(auto_now_add=True)
    views = models.PositiveIntegerField(default=0)

    def __str__(self):
        return self.subject

    class Meta:
        db_table = 'topic'
        verbose_name = 'topics'


class PostModel(models.Model):
    message = models.TextField()
    topic = models.ForeignKey(TopicModel, related_name='posts', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(null=True)
    created_by = models.ForeignKey(CustomUser, related_name='posts', on_delete=models.CASCADE)
    updated_by = models.ForeignKey(CustomUser, null=True, related_name='+', on_delete=models.CASCADE)

    def __str__(self):
        truncated_message = Truncator(self.message)
        return truncated_message.chars(30)

    class Meta:
        db_table = 'post'
        verbose_name = 'posts'