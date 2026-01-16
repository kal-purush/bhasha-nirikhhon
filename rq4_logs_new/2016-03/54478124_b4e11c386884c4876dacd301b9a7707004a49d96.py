# -*- coding: utf-8 -*-

import urllib2
import requests

class Wiki:
    """ Simplifie l'acces a wiki """
    def __init__(self, user, password, baseurl='http://wikipast.world/wiki/', summary='Wiki Access'):
        """
        :param user: nom d'utilisateur du bot
        :param password: mot de passe du bot
        :param baseurl: url du wiki
        :param summary: summary du bot (message associé aux modifications qu'il apporte)
        """
        passw=urllib2.quote(password)
        login_params='?action=login&lgname=%s&lgpassword=%s&format=json'% (user,passw)
        # Login request
        r1=requests.post(baseurl+'api.php'+login_params)
        login_token=r1.json()['login']['token']

        #login confirm
        login_params2=login_params+'&lgtoken=%s'% login_token
        r2=requests.post(baseurl+'api.php'+login_params2,cookies=r1.cookies)

        #get edit token2
        params3='?format=json&action=query&meta=tokens&continue='
        r3=requests.get(baseurl+'api.php'+params3,cookies=r2.cookies)
        self.edit_token=r3.json()['query']['tokens']['csrftoken']

        self.edit_cookie=r2.cookies.copy()
        self.edit_cookie.update(r3.cookies)

