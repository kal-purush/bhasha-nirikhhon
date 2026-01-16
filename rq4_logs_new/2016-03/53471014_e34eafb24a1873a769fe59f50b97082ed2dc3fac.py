import os


def get_whois(url):
    command = 'whois ' + url
    process = os.popen(command)
    print(process)
    results = str(process.read())
    return results

print(get_whois('reddit.com'))