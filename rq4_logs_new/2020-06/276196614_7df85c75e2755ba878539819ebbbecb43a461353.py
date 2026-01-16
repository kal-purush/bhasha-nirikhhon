from django.contrib.auth import get_user_model
from polls.models import Choice, Question
from django.utils import timezone

User = get_user_model()
User.objects.create_superuser('admin', 'admin@myproject.com', 'password')

q = Question(question_text='Cats or dogs?', pub_date=timezone.now())
q.save()
q.choice_set.create(choice_text='Cats', votes=0)
q.choice_set.create(choice_text='Dogs', votes=0)