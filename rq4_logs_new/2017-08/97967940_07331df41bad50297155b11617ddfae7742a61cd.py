from benchmarker import Benchmarker, AsyncBenchmark
import datetime
import json
import urllib
import urllib2
import time
import random
import logging

logger = logging.getLogger(__name__)


class OpenTSDBBenchmarker(Benchmarker):
    def get_domain_benchmarker(self):
        return OpenTSDBDomainBenchmark()


class OpenTSDBBaseBenchmark(AsyncBenchmark):
    def _insert_data(self, data):
        chunk = 25
        for i in range(0,len(data), chunk):
            url = 'http://localhost:4242/api/put?details=1'
            request = urllib2.Request(url,
                                      headers={'Content-Type': 'application/json'},
                                      data=json.dumps(data[i:i+chunk]))
            opener = urllib2.build_opener()
            try:
                opener.open(request)
            except urllib2.HTTPError as e:
                logger.exception(e)

    def _validate_data(self, expected, metric):
        url = 'http://localhost:4242/api/query?start={}&m=sum:{}'.format(time.time()-1*60*60, metric)
        request = urllib2.Request(url)
        opener = urllib2.build_opener()
        logger.info(opener.open(request).read())

class OpenTSDBDomainBenchmark(OpenTSDBBaseBenchmark):
    def insert_data(self, iterator):
        return self.insert_async({
            'metric': 'domains',
            'timestamp': time.time(),
            'value': 1,
            'tags': {
                'domain': next(iterator)
            }
        })

    def query_data(self):
        url = 'http://localhost:4242/api/query/gexp?start={}&end={}&exp=highestCurrent(sum:1m-sum-none:domains{{domain=*}}, 5)'
        # Send request ten times
        now = time.time() - 60*10
        start = time.time()
        try:
            opener = urllib2.build_opener()
            for i in range(10):
                req = url.format(now+i*60,now+(i+1)*60)
                #logger.info(req)
                logger.info(urllib.urlopen(req).read())
        except urllib2.HTTPError:
            logger.error('Error reading data, this can happen the first time when no data have been inserted')
            logger.error(req)
        except Exception as e:
            logger.error(req)
            logger.exception(e)
        return time.time() - start

        url = 'http://localhost:4242/api/query?start={}&m=sum:1m-sum-none:domains{{domain=*}}'.format(time.time() - 60*10)
        request = urllib2.Request(url, headers={"Accept-Encoding": "gzip, deflate"})
        opener = urllib2.build_opener()
        start = time.time()
        try:
            logger.info('querying')
            logger.info(url)
            opener.open(request)
            logger.info('querying done')
        except urllib2.HTTPError as e:
            logger.error('Error reading data, this can happen when no data have been inserted')
            logger.error(url)
            logger.exception(e)

        return time.time() - start

    def validate_data(self, expected):
        return self._validate_data(expected, 'domain')

if False:
    from load_iterators import generate_random_domain_iterator
    logging.basicConfig()
    it = generate_random_domain_iterator()
    b = OpenTSDBDomainBenchmark()
    b.initialize()
    b.insert_data(it)
    b.query_data()
    print('end')

if False:
    url = 'http://localhost:4242/api/put?details=1'
    while True:
        data = []
        data.append({
            'metric': 'domains',
            'timestamp': time.time(),#int(time.mktime(datetime.datetime.now().utctimetuple())),
            'value': 2,
            'tags': {
                'domain': 'test{}.cl'.format(random.randint(0,5))
            }
        })
        for i in range(4333):
            data.append({
                'metric': 'domains',
                'timestamp': time.time(),#int(time.mktime(datetime.datetime.now().utctimetuple())),
                'value': 2,
                'tags': {
                    'domain': 'test{}.cl'.format(random.randint(0,5))
                }
            })
        data = json.dumps(data)
        request = urllib2.Request(url,
                                  headers={'Content-Type': 'application/json'},
                                  data=data)
        opener = urllib2.build_opener()
        import httplib
        try:
            r = opener.open(request)
            print r.read()
        except urllib2.HTTPError as e:
            print e.read()
        except httplib.BadStatusLine as e:
            print e
        time.sleep(1)

        url2 = 'http://localhost:4242/api/query?start={}&m=sum:1m-sum-none:domains{{domain=*}}'.format(time.time() - 60*10)
        try:
            request = urllib2.Request(url2)
            r = opener.open(request)
            print r.read()
        except urllib2.HTTPError as e:
            print e.read()
        time.sleep(1)

    #http://localhost:4242/q?start=2017/08/03-17:45:00&ignore=33&m=sum:domains{domain=test0.cl}&o=&yrange=[0:]&wxh=820x753&style=linespoint&json