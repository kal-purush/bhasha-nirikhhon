from ConfigParser import SafeConfigParser
from datetime import datetime
from praw import Reddit

import PIL
from PIL import ImageFont
from PIL import Image
from PIL import ImageDraw

def send_header(img, reddit, subreddit):
    img.save("header_cd.png")
    reddit.upload_image(subreddit, "header_cd.png", name=None, header=True)


def apply_text_on_image(image, text, font):
    img = Image.open(image)
    font = ImageFont.truetype(font, 25)
    draw = ImageDraw.Draw(img)
    draw.text((535, 50),text,(250,250,255),font=font)
    draw = ImageDraw.Draw(img)
    return img

def remaining_time(target):
    diff = target - datetime.now()
    days = diff.days
    hours = diff.seconds // 3600
    minutes = (diff.seconds - (hours * 3600)) // 60
    seconds = (diff.seconds - (hours * 3600) - (minutes * 60))

    if seconds < 10:
        seconds = "0%s" % seconds

    if minutes < 10:
        minutes = "0%s" % minutes

    if hours < 10:
        hours = "0%s" % hours

    return {
        'seconds': seconds,
        'minutes': minutes,
        'hours': hours,
        'days': days
    }

def format_time_simpler(timediff):
    return "%s days, %sh %sm" % (timediff['days'], timediff['hours'], timediff['minutes'])


conf = SafeConfigParser()
conf.read('settings.cfg')

username = conf.get('reddit', 'username')
password = conf.get('reddit', 'password')
subreddit = conf.get('reddit', 'subreddit')
target = conf.get('image', 'target')
image_location = conf.get('image', 'image')
font = conf.get('image', 'font')


r = Reddit(user_agent='linux:net.dosaki.subreddit_header_countdown:0.0.1 (by /u/dosaki)')
r.login(username, password)

target_datetime = datetime.strptime(target, '%Y %m %d %H:%M:%S')
timediff = remaining_time(target_datetime)

image = apply_text_on_image(image_location, format_time_simpler(timediff), font)
send_header(image, r, subreddit)