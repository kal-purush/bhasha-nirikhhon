from allauth.account.adapter import DefaultAccountAdapter

class CustomAccountAdapter(DefaultAccountAdapter):

    def save_user(self, request, user, form, commit=False):
        data = form.cleaned_data
        user = super().save_user(request, user, form, commit)
        
        user.name = data.get('name')
        user.save()
        return user