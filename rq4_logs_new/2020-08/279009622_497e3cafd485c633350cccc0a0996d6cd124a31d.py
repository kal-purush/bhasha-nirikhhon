from django.conf import settings
from django.db import models
from core.mixins import TimeStampMixin


class Answer (TimeStampMixin):
    title = models.CharField(max_length=100)
    message = models.TextField()
    up_votes = models.PositiveIntegerField(default=0)
    down_votes = models.PositiveIntegerField(default=0)
    question = models.ForeignKey(
        'Question', on_delete=models.CASCADE
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE
    )