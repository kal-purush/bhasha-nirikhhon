from django.http import HttpResponse, HttpResponseRedirect


def index(request):
	return HttpResponse("You entered the index page of freesources")