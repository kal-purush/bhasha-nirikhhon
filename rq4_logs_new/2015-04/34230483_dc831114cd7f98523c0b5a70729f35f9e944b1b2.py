from django.forms import ModelForm
from tasks.models import Tasks

class TasksForm(ModelForm):
    class Meta:
        model = Tasks
        fields = ['description', 'complete']
