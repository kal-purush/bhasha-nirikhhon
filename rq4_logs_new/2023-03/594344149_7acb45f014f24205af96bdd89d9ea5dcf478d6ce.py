from django.db import models
from django.contrib.auth import get_user_model
import uuid

User = get_user_model()

class Rank(models.Model):
    id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey(User, on_delete=models.CASCADE, related_name='ranks', db_column='user_id')
    rank = models.IntegerField(null=True)
    score = models.IntegerField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_deleted = models.BooleanField(default=False)

    class Meta:
        db_table = 'rank'

## Django에서는 모델의 이름을 단수형으로 사용한다. 따라서 rank가 적합