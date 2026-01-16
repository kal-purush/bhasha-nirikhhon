# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'zday',
        'USER': 'zday',
        'PASSWORD': '3231',
        'HOST': 'pgdb',
        'PORT': 5432,
    }
}