import os
import argparse
import datetime
import pdb
import requests
import configparser
import webbrowser
from tqdm import tqdm

def make_gpx_line(lat, lon, time):
    return '\t\t<trkpt lat="{}" lon="{}"><time>{}</time></trkpt>\n'.format(lat, lon, time)

parser = argparse.ArgumentParser()

parser.add_argument("-s", "--start", dest="startDate", help="write report to FILE", default=datetime.date.today().strftime("%Y-%m-%d"))
parser.add_argument("-e", "--end", dest="endDate", help="write report to FILE", default=datetime.date.today().strftime("%Y-%m-%d"))

args = parser.parse_args()
config = configparser.RawConfigParser()

if not os.path.isfile('config.ini'):
    print('''
No config file was found. We'll make one for you.
''')
    config.add_section('main')
    config.set('main', 'client_id', '')
    config.set('main', 'client_secret', '')
    config.set('main', 'access_token', '')

    with open('config.ini', 'w') as f:
        config.write(f)

    
config.read('config.ini')

if not config['main'].get('client_id') or not config['main'].get('client_secret'):
    print('''
The config file is missing either the client ID or the client secret, so we
can't do anything yet. Please follow the instructions in README.md to get
those, set them in the config file, and then run this again.
''')


elif not config['main'].get('access_token'):
    print('''
No access token is set in the config file, so we'll have to request one.
Next, we'll open the Moves app authorization page in your web browser. Follow
the instructions there to get your auth code, and once you get that, copy it
and paste it into the next prompt.
''')

    input("Press any key to continue or 'q' to quit.")

    url = "https://api.moves-app.com/oauth/v1/authorize?response_type=code&client_id=" + config['main']['client_id'] + "&scope=activity%20location"
    webbrowser.open(url)

    auth = input('Enter the authorization code: ')
    r = requests.post('https://api.moves-app.com/oauth/v1/access_token', {'grant_type': 'authorization_code', 'code': auth, 'client_id': config['main']['client_id'], 'client_secret': config['main']['client_secret']})
    
    if r.status_code == 200:
        r = r.json()
        config.set('main', 'access_token', r['access_token'])

        with open('config.ini', 'w') as f:
            config.write(f)


if config['main'].get('access_token'):
    startDate = datetime.datetime.strptime(args.startDate, "%Y-%m-%d")
    endDate = datetime.datetime.strptime(args.endDate, "%Y-%m-%d")
    delta = endDate - startDate

    gpx = '''
<?xml version="1.0"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<trk>
    <name>output</name>
    <trkseg>
'''

    for i in tqdm(range(delta.days + 1)):
        currentDate = (startDate + datetime.timedelta(days = i - 1)).strftime("%Y-%m-%d")
        r = requests.get('https://api.moves-app.com/api/v1/user/storyline/daily/' + currentDate, {'trackPoints': 'true', 'access_token': config['main']['access_token']})

        r = r.json()

        for segment in r[0]['segments']:
            if segment['type'] == 'place':
                gpx += make_gpx_line(segment['place']['location']['lat'], segment['place']['location']['lon'], segment['startTime'])
                gpx += make_gpx_line(segment['place']['location']['lat'], segment['place']['location']['lon'], segment['endTime'])
            elif segment['type'] == 'move':
                for activity in segment['activities']:
                    for point in activity['trackPoints']:
                        gpx += make_gpx_line(point['lat'], point['lon'], point['time'])

    gpx += '\t</trk>\n</gpx>'

    output = open('out.gpx', 'w')
    output.write(gpx)
    output.close()

    print("Done!")