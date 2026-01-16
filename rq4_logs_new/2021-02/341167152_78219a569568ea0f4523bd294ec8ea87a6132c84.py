from django.shortcuts import render


def home(request):
    return render(request, 'home.html')

def produkt1(request):
    return render(request, 'produkt1.html')

def produkt2(request):
    return render(request, 'produkt2.html')

def produkt3(request):
    return render(request, 'produkt3.html')

def informacje_o_zamowieniu(request):
    return render(request, 'infozam.html')

def reklamacje(request):
    return render(request, 'reklamacje.html')

def kontakt(request):
    return render(request, 'kontakt.html')

def co_sprzedajemy(request):
    return render(request, 'cosprzedajemy.html')