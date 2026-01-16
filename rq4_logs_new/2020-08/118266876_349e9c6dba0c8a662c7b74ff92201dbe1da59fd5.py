#!/python3

import pickle
import lj
import re
import wpparser
import time
import requests
import subprocess

# REQUIREMENTS.TXT
# lj
# wpparser



# path to an exported dump of your wordpress after its imported the LJ data (I couldn't be bothered fighting OAUTH)
wordpress_export_filename = "WORDPRESSXMLFILE"

# login details to your livejournal
lj_user = 'LJUSERNAME'
lj_pass = 'LJPASSWORD'



def connect_lj(user, password):
    from lj import lj as _lj
    lj = _lj.LJServer("Python-Blog3/1.0", "http://daniil-r.ru/bots.html; i@daniil-r.ru")
    lj.login(user, password)

    return lj


def get_lj_item_ids(lj):
    item_ids = set()
    last_entry = None
    count = 0
    total = None
    while count != total:
        sync = lj.syncitems(last_entry)
        count = sync['count']
        total = sync['total']

        for item in sync['syncitems']:
            if not item['item'].startswith('L-'):
                continue

            item_ids.add(int(item['item'][2:]))
            last_entry= item['time']

    return item_ids


def get_lj_items(lj,posts):
    lj_item_ids = get_lj_item_ids(lj)

    for item_id in lj_item_ids:
        while True:
            try:
                item = lj.getevents_one(itemid=item_id)['events'][0]
                break
            except Exception as ex:
                print("Failed, sleeping and retrying: {}".format(ex))
                time.sleep(60)
                pass

        item_date = item['eventtime']

        posts.setdefault(item_date, {})
        posts[item_date]['lj'] = item


def get_wordpress_items(wordpress_export_filename, posts):
    data = wpparser.parse(wordpress_export_filename)
    for post in data['posts']:
        if post.get('post_type') != 'post':
            continue
        post_date = post['post_date']

        posts.setdefault(post_date, {})
        posts[post_date]['wp'] = post


def get_all_items(lj):
    try:
        with open('/tmp/posts', 'rb') as f:
            posts = pickle.loads(f.read())
        return posts
    except FileNotFoundError:
        pass

    posts = {}
    get_wordpress_items(wordpress_export_filename, posts)
    get_lj_items(lj, posts)

    try:
        with open('/tmp/posts', 'wb') as f:
            f.write(pickle.dumps(posts))
    except FileNotFoundError:
        pass

    return posts


def delete_zapped_items_from_lj(lj, posts):
    for post_date, post_data in posts.items():
        # we keep items which are present in wordpress
        if post_data.get('wp'):
            continue

        arguments = lj._LJServer__headers()
        arguments['itemid'] = post_data['lj']['itemid']

        try:
            response = lj._LJServer__request('editevent', arguments)
        except Exception as ex:
            print(ex)


def link_lj_items_to_new_blog(lj, posts):
    for post_date, post_data in posts.items():
        # we skip items which are not in lj or not in wp
        if not post_data.get('lj') or not post_data.get('wp'):
            continue

        event = str(post_data['lj']['event'])
        if 'this blog has moved' in event.lower():
            continue

        event = '<span style="font-size: 1.4em"><a href="{}">This blog has moved</a></span>\r\n\r\n'.format(post_data['wp']['link']) + event

        arguments = lj._LJServer__headers()
        arguments['itemid'] = post_data['lj']['itemid']
        arguments['subject'] = post_data['lj']['subject']
        arguments['event'] = event
        try:
            response = lj._LJServer__request('editevent', arguments)
        except Exception as ex:
            print(ex)


def fixup_posts():
    # do it!
    lj = connect_lj(lj_user, lj_pass)
    posts = get_all_items(lj)
    delete_zapped_items_from_lj(lj, posts)
    link_lj_items_to_new_blog(lj, posts)


def find_lj_links():
    posts = {}
    get_wordpress_items(wordpress_export_filename, posts)

    found_images = set()
    found_posts = set()
    ljpicre = re.compile('(http://pics.livejournal.com/[^/]+/pic/[0-9a-zA-Z]+/)')
    for k, v in posts.items():
        if 'wp' not in v:
            continue
        wp = v['wp']
        content = wp['content']

        if 'livejournal' in content:
            print(wp)

        matches = ljpicre.finditer(content)
        for match in matches:
            # need to convert the lj image url into one we can recognise
            url = match.group(1).replace('http:', 'https:').strip('/')
            r = requests.get(url, allow_redirects=False)
            if r.status_code == 302:
                # print(wp['link'], url, r.headers['Location'])
                found_images.add(r.headers['Location'])
                found_posts.add(wp['link'])
            elif r.status_code == 404:
                print(wp['link'], url, 'DEAD')
            else:
                print(wp['link'], url, '?????')

    # for img in found_images:
    #     subprocess.check_call(['wget', img])
    for post in found_posts:
        print(post)

# fixup_posts()
find_lj_links()