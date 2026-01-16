#!C:/Python36/python.exe
# -*- coding: utf-8 -*-

"""
 * Страница авторизации для проекта Стена сообщений (The Wall of Messages)
 * Проект разработан в порядке выполнения тестового задания на вакансию веб-разработчик в компании Light IT
 *
 * Copyright (C) 2017 Alexander Malygin
 *
"""

import cgi
from os import environ as env
from fbauth import *   # модуль авторизации
from thewall import *  # модуль ядра системы

# параметры авторизации
APP_ID   = '121474638496991'
APP_KEY  = '9b600471ac1fe1dde32738b24540bf47'
AUTH_URL = '%s://%s%s' % (env['REQUEST_SCHEME'], env['HTTP_HOST'], env['REQUEST_URI'].split('?', 1)[0])

# объект авторизации на Facebook
fb = FBAuth(APP_ID, APP_KEY, AUTH_URL) # http://thewall.avm/auth.py

# обратная связь от Facebook API
code = cgi.FieldStorage().getvalue("code")
if (code): fb.auth(code) 

if fb.auth_status: # успешная авторизация, переход на страницу стены
  tw = TheWall()
  name = ' '.join([fb.user_info['first_name'], fb.user_info['last_name']])
  uid = tw.updateUser(fb.user_info['id'], name)
  print("Content-type: text/html; charset=utf-8" )
  print("Location: show.py?uid=%s" % (uid))
  print()
else:              # авторизация не состоялась, надо показать страницу авторизации
  print("Content-type: text/html; charset=utf-8\n")
  with open('thewall_auth.html') as f: print(f.read().replace('%fblink%', fb.getLink()))