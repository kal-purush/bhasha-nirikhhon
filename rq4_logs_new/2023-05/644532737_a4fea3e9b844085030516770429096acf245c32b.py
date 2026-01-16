#code for admin site

#importing the model we created
from .models import Movie

from django.contrib import admin

#adds database to admin site. Adds to site on server startup (so refresh).
admin.site.register(Movie)