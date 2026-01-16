from django.conf import settings # import the settings file

def company_name(request):
    # return the value you want as a dictionnary. you may add multiple values in there.
    return {'COMPANY_NAME': settings.COMPANY_NAME}