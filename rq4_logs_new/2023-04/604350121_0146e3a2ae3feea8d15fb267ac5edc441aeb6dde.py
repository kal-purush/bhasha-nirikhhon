# views.py
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login
from django.shortcuts import render, redirect
from django.views import View

class LoginView(View):
    template_name = 'login/login.html'
    
    def get(self, request):
        error_msg = ''
        return render(request, self.template_name, {'error_msg': error_msg})
    
    def post(self, request):
        username = request.POST['username']
        password = request.POST['password']
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('/') # replace 'home' with the name of your home page
        else:
            error_msg = 'Invalid login credentials'
            return render(request, self.template_name, {'error_msg': error_msg})

class RegisterView(View):
    template_name = 'login/register.html'

    def get(self, request):
        return render(request, self.template_name)

    def post(self, request):
        username = request.POST['username']
        password = request.POST['password']
        user = User.objects.create_user(username=username, password=password)
        user.save()
        return redirect('/login')

    