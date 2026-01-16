import requests

base_url = 'http://169.254.169.254/latest/meta-data/{}'

instance_id = requests.get(base_url.format('instance-id')).content
public_ip = requests.get(base_url.format('public-ipv4')).content

page = """\
<html>
    <head>
        <title>My instance info</title>
    </head>
    <body>
        <p>My instance id is: {}</p>
    </body>
</html>""".format(instance_id, public_ip)

with open('/var/www/html/index.html', 'w') as fd:
    fd.write(page)