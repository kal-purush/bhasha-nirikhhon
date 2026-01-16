import time
import pandas as pd
import certifi
import urllib3
import json
from urllib3.exceptions import ReadTimeoutError, ConnectTimeoutError

def try_wait(remain, st):
    print(remain)
    if 10 > int(remain):
        print('###### WAITING FOR LIMITATION ######')
        time.sleep(st + 3600 - time.time())
        new_st = time.time()
        return new_st
    if 4990 < int(remain):
        print('############ NEW HOUR ##############')
        new_st = time.time()
        return new_st
    return st

def getURL(url):
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'

    headers = [	urllib3.util.make_headers(user_agent = user_agent, basic_auth = 'cit-bot1:sjtucit1'), \
    			urllib3.util.make_headers(user_agent = user_agent, basic_auth = 'cit-bot2:sjtucit2'), \
    			urllib3.util.make_headers(user_agent = user_agent, basic_auth = 'cit-bot3:sjtucit3'), \
    			urllib3.util.make_headers(user_agent = user_agent, basic_auth = 'cit-bot4:sjtucit4'), \
    			urllib3.util.make_headers(user_agent = user_agent, basic_auth = 'cit-bot5:sjtucit5') ]
    st = time.time()

    for header in headers:
        try:
            r = http.request('GET', url, timeout=urllib3.Timeout(connect=2.0, read=5.0), retries=False, headers=header)
        except ReadTimeoutError:
                print("ReadTimeoutError")
                continue
        except ConnectTimeoutError:
                print("ConnectTimeoutError")
                continue
        except e:
                print(e)
                continue
        else:
            #print(r.status)
            new_st = try_wait(r.headers['X-RateLimit-Remaining'], st)
            if new_st != st:
                st = new_st
            returnData = json.loads(r.data)
            return returnData
    # count = count + 1
    # url = r.headers['Link'].split('<')[1].split('>')[0]