# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf.urls import url, include
from views import sayHello, sayHi

urls = [
           url(r'^hello/', sayHello),
           url(r'^hi/', sayHi),
       ]