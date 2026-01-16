from django.shortcuts import render
from django.http import HttpResponse


def index(request):
		return render(request, "index.html")
		
def contato(request):
	email = request.POST.get('email')
	text = request.POST.get('nome')
	
	if request.method == "GET":
		return render(request,'contato.html')
	else:
		print("E-mail:",email)
		print("Senha:",text)
		
		
def login(request):
	if request.method == "GET":
		return render(request, 'login.html')
	else:
		email = request.POST.get('email')
		password = request.POST.get('password')
		
		if password == 'teste123':
			print("Usuário",email,"entrou com sucesso!")
			return render(request, "index.html")
		else:
			print("Usuário",email," digitou a senha errada!")
			return render(request, 'login.html')		
		
	
		
		
		
		

		
		

		
		
		
		
		
		