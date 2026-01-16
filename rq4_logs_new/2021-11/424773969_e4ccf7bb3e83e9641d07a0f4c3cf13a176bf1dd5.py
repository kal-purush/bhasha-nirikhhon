#!/usr/bin/env python3

import json
import requests.auth
import time
import re
import requests
import cv2
import pytesseract
from datetime import datetime
import os

def playsound():
        os.system('spd-say "code potentially found"')


# Define print
def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

pytesseract.pytesseract.tesseract_cmd='/home/thejhoule/projects/redditcraw/venv/bin/pytesseract'

with open('config.txt','r') as f:
    login = dict(x.rstrip().split('=',1) for x in f)

newurl1 = 'https://oauth.reddit.com/r/MagicArena/new'
newurl2 = 'https://oauth.reddit.com/r/magicTCG/new'
reqtok = "https://www.reddit.com/api/v1/access_token"

auth = requests.auth.HTTPBasicAuth(login['appid'], login['secrt'])
data = {'grant_type': 'password',
        'username': login['name'],
        'password': login['pass']}
headers = {'User-Agent': 'PreReleaseCodesHunter/0.0.1'}

i=0
params1={}
params2={}
# Keyword list to search for in re terminology
termlist=['extra','prerelease','enjoy!?\b','codes?','spare','kaldheim.*codes?','pre release','pre-release','free.*packs?',
          'promo','don\'?t.*play.*arena']
# To record history of posts
historytitl=''

# ##SETUP END## #

while True:

    # Requests new Token when old token expires
    res = requests.post(reqtok, auth=auth, data=data, headers=headers)
    t = time.time()
    Val = float(res.json()['expires_in'])
    Token = res.json()['access_token']
    headers = {**headers, **{'Authorization': f"bearer {Token}"}}

    # Jumps out of loop when Token is about to expire
    while time.time()-t < Val-60:

        # Requests data from r/MagicArena
        lastposts1 = requests.get(newurl1, headers=headers, params=params1 )
        # Requests data from r/magicTCG
        lastposts2 = requests.get(newurl2, headers=headers, params=params2)
        # Concatenates data
        thelist = lastposts1.json()['data']['children'] + lastposts2.json()['data']['children']
        # Update "before" tags from both requests
        before1 = lastposts1.json()['data']['before']
        before2 = lastposts2.json()['data']['before']
        params1={'before':f'{before1}'}
        params2={'before': f'{before2}'}

        for x in thelist:

            k=False
            titlematch = []
            bodymatch = []

            # Write instance title and remove special Re characters
            title = x['data']['title']
            title = re.sub('\)', '', title)
            title = re.sub('\(', '', title)
            title = re.sub('\?', '', title)
            title = re.sub('\?', '', title)
            title = re.sub('\+', '', title)
            title = re.sub('\*', '', title)
            title = re.sub('\.', '', title)
            title = re.sub('\$', '', title)
            title = re.sub('\|', '', title)
            title = re.sub('\{', '', title)
            title = re.sub('}', '', title)
            title = re.sub('\^', '', title)
            title = re.sub(']', '', title)
            title = re.sub('\[', '', title)

            # Write URL, Post body and time of post
            url = x['data']['url']
            body = x['data']['selftext']
            postime = x['data']['created']

            # Search title for keywords
            for d in termlist:
                if re.search(d,title, re.I) and re.search(title,historytitl) == None:
                    titlematch.append(re.search(d,title, re.I).group())

            # Print title, time and link of the post
            if len(titlematch) != 0 and re.search(title,historytitl) == None:
                k = True
                print('Match at post: ' + f'{title}'+f'. At time: {str(datetime.fromtimestamp(postime))}\n')
                print('Link: ' + f' {url}\n')
                playsound()

            # Search body for codes
            if (len(re.findall('..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?', body, re.I)) != 0) and re.search(title,historytitl) == None:
                bodymatch.append(re.findall('..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?', body, re.I))

            # Print post title, time, body text and extracted codes
            if len(bodymatch)!=0 and re.search(title,historytitl) == None:
                k = True
                print('Match at post: ' + f'{title}'+f'. At time: {str(datetime.fromtimestamp(postime))}\n')
                print('raw text: '+f'{body}\n')
                print('Link: ' + f' {url}\n')
                print('codes found: '+f'{bodymatch}\n')

                playsound()


            # Check if there's a preview entry on post dictionary
            if 'preview' in x['data']:

                # Check for images at the 'preview' entry
                if (len(titlematch) != 0 or len(bodymatch) != 0) and x['data']['preview']['enabled']:
                    imgurl = None
                    imgurl = x['data']['preview']['images'][0]['source']['url']
                    imgurl = re.sub('amp;', '', imgurl)
                    print(f'Image link: {imgurl}')

                    format = re.search('\.\D\D\D\D?\?', imgurl).group()
                    format = re.sub('\?', '', format)

                    img_data = requests.get(imgurl).content
                    img_name=f'image{format}'

                    with open(img_name, 'wb') as handler:
                        handler.write(img_data)

                    image = cv2.imread(img_name)
                    text = pytesseract.image_to_string(image)
                    codefromimg = re.findall('..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?', text, re.I)

                    if codefromimg:
                        print('Match at post: ' + f'{title}\n')
                        print('Link: ' + f' {url}\n')
                        print('codes found: ' + f'{codefromimg}\n')
                        playsound()


            if (len(titlematch) != 0 or len(bodymatch) != 0) and ('media_metadata' in x['data']):

                imgs =list( x['data']['media_metadata'].keys() )

                for ig in imgs:
                    imgurl = x['data']['media_metadata'][ig]['s']['u']
                    imgurl = re.sub('amp;', '', imgurl)
                    print(f'Image link: {imgurl}')

                    format = re.search('\.\D\D\D\D?\?', imgurl).group()
                    format = re.sub('\?', '', format)

                    img_data = requests.get(imgurl).content
                    img_name = f'image{format}'

                    with open(img_name, 'wb') as handler:
                        handler.write(img_data)

                    image = cv2.imread(img_name)
                    text = pytesseract.image_to_string(image)
                    # print(text)
                    codefromimg = re.findall('..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?-..?.?.?.?.?', text, re.I)

                    if codefromimg:
                        print('Match at post: ' + f'{title}\n')
                        print('Link: ' + f' {url}\n')
                        print('codes found: ' + f'{codefromimg}\n')
                        playsound()


            # Writes title in title history to prevent accounting for it again
            if re.search(f'{title}', historytitl) == None:
                #print(title)
                historytitl += str(title) + ' ; '

            # Counts No of posts with matching keys
            if k :
                i+=1

        # Prints number of analysed posts as No of found posts
        analyz=len(re.split(';',historytitl))-1
        print(f'Posts found: {i}. Posts analysed {analyz}')

        # Repeat the request in 4 seconds (to not flood API server)
        time.sleep(4)
