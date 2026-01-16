# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
# ===================================================================================

def get_prices(texto):
    ""
    texto = texto.lower()
    currs = {'€', '£', '$', 'dkk', 'rub'}
    cur = list(filter(lambda cr: cr in texto, currs))[0]

    keep_prices = []
    str_prices = []
    priceslong = texto.split(cur)
    for pr in priceslong:
        if 'return' not in pr:

            if cur != 'dkk':
                prrs = pr.strip().split(' ')[0]
                if (prrs[-1] == '.') or (prrs[-1] == ','):
                    prrs_clean = prrs[:-1]
                else:
                    prrs_clean = prrs
                try:
                    keep_prices.append(float(prrs_clean))
                    str_prices.append(prrs_clean)
                except:
                    pass
            else:
                prrs = pr.strip().split(' ')[-1]
                if (prrs[0] == '(') or (prrs[0] == '['):
                    prrs_clean = prrs[1:]
                else:
                    prrs_clean = prrs
                try:
                    keep_prices.append(float(prrs_clean))
                    str_prices.append(prrs_clean)
                except:
                    pass

    info = []
    for prr in str_prices:
        for stce in texto.split('. '):
            if cur != 'dkk':
                if (cur + ' ' + prr) in stce:
                    info.append(stce.replace('\xa0', ' '))
            else:
                if (prr + ' ' + cur) in stce:
                    info.append(stce.replace('\xa0', ' '))


    out_info = list(set(info))
    return {'prices': keep_prices, 'sources': out_info, 'currency':cur}

def estimate_transportation_prices(city='', airport=''):

    trasp_web = requests.get("https://airmundo.com/en/airports/{}-{}-airport/transportation/".format(city, airport).lower())

    if trasp_web.status_code == 404:
        trasp_web = requests.get("https://airmundo.com/en/airports/{}-{}-airport/transportation/".format(city,city).lower())

    if trasp_web.status_code == 404:
        trasp_web = requests.get("https://airmundo.com/en/airports/{}-airport/transportation/".format(airport).lower())

    if trasp_web.status_code == 404:
        trasp_web = requests.get("https://airmundo.com/en/airports/{}-airport/transportation/".format(city).lower())

    if trasp_web.status_code == 404:
        raise ValueError("Page not loaded: 404")


    soup = BeautifulSoup(trasp_web.content, 'html.parser')

    strongs = list(soup.find_all('strong'))
    heads = [item.text for item in strongs]
    keeps = list(filter(lambda tm: 'to {}'.format(city.lower()) in tm.lower(), heads))

    outdicts = []
    for keep in keeps:
        text = soup.find_all('strong', text=keep)[0].parent.get_text()
        dct = get_prices(text)
        dct.update({'head':keep})
        outdicts.append(dct)

    return outdicts
