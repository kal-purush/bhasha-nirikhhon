ALLOWED_HOSTS = ["*"]

# Modules in use, commented modules that you won't use
MODULES = [
    "event",
    "serie",
    "ticket",
    "equipment",
    "routine",
    "client",
    "gym",
    "owner",
    "user",
    "workout",
    "reservation",
    "assessment",
]
BASEURL = 'http://localhost:8000'
APIS = {
    'event': BASEURL,
    'serie': BASEURL,
    'ticket': BASEURL,
    'equipment': BASEURL,
    'routine': BASEURL,
    'client': BASEURL,
    'gym': BASEURL,
    'owner': BASEURL,
    'user': BASEURL,
    'workout': BASEURL,
    'reservation': BASEURL,
    'assessment': BASEURL,
}



DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'muscledb',
        'USER': 'muscleuser',
        'PASSWORD':'musclepass123',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}

# number of bits for the key, all auths should use the same number of bits
KEYBITS = 256