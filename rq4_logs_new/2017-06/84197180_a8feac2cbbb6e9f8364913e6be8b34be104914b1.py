# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.conf import settings
from django.utils import translation
import logging
class ForceLangMiddleware:

    def process_request(self, request):
        if not translation.LANGUAGE_SESSION_KEY in request.session \
           or not request.session[translation.LANGUAGE_SESSION_KEY] \
           and settings.LANGUAGE_CODE:
            translation.activate(settings.LANGUAGE_CODE)
            request.session[translation.LANGUAGE_SESSION_KEY] = settings.LANGUAGE_CODE
            logging.warning("Activated default lang '{}'".format(settings.LANGUAGE_CODE))