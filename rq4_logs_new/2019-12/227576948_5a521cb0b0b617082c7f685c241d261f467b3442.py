###########################################
###    Source code by - Strzelczyk    ###
### Zdjęcia Dostarczały różne grupy   ###
###########################################

import requests
import json
import random

imagesnum = 30

def randomHentai(limit: int=imagesnum):
    r = random.randint(1, limit)
    rh = f"https://MisTenNajaranyh.github.io/MisTenNajaranyh/images/{r}.png"
    return rh

def sendWebhook(url: str, name: str=None, avatar: str=None):
    r = random.randint(1, imagesnum)
    rh = f"https://MisTenNajaranyh.github.io/MisTenNajaranyh/images/{r}.png"

    embeds = [
        {
            "image": {
                "url": rh
            }
        }
    ]

    h = {
        "content-type": "application/json"
    }
    
    p = {
        "username": name,
        "avatar_url": avatar,
        "embeds": embeds
    }

    return requests.post(url, headers=h, json=p)