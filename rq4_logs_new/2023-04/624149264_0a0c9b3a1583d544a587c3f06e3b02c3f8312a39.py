import pycurl
try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode
from urllib.request import urlopen
import json
import time
def sensor_do(s, url, pf, buf):
    s.setopt(s.URL, url)
    s.setopt(s.POSTFIELDS, pf)
    s.setopt(s.WRITEDATA, buf)
    s.perform()
    rcode = s.getinfo(s.RESPONSE_CODE)
    success = rcode in range(200, 207)
    print('%s %s: %d (%s)' % (url, pf, rcode, 'OK' if success else 'ERROR'))
    return success
Base_URL = 'http://192.168.1.201/cgi/'
sensor = pycurl.Curl()
buffer = BytesIO()
rc = sensor_do(sensor, Base_URL+'reset', urlencode({'data':'reset_system'}), buffer)
if rc:
    time.sleep(10)
    rc = sensor_do(sensor, Base_URL+'setting', urlencode({'rpm':'300'}), buffer)
if rc:
    time.sleep(1)
    rc = sensor_do(sensor, Base_URL+'setting', urlencode({'laser':'on'}), buffer)
if rc:
    time.sleep(10)

response = urlopen(Base_URL+'status.json')

if response:
    status = json.loads(response.read())
    print('Sensor laser is %s, motor rpm is %s' % (status['laser']['state'], status['motor']['rpm']))
sensor.close()