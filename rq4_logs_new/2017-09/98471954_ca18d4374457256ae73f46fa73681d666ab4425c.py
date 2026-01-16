# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json

from django.http import HttpResponse


class json_response():

    def dic_to_httpresponse(self, pdic):
        return HttpResponse(
            json.dumps(pdic),
            content_type='application/json; charset=UTF-8'
        )

    def response_ok(self):
        return self.dic_to_httpresponse({'status': 'ok'})

    def response_error_form(self, form):
        error_list = []
        for key, value in form.errors.items():
            error_list.append((key, value))
        response = {
            'status': 'error',
            'errors': error_list
        }
        return self.dic_to_httpresponse(response)