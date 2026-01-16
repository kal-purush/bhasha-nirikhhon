from relier.web.views import AuthenticatedView
from flask import render_template, request, flash, redirect, session
from relier.models import User

class Logout(AuthenticatedView):

    def get(self):

        session.pop('token', None)
        session.modified = True

        return redirect('/')