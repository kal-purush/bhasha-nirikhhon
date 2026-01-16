import requests

s = None


def fetch_session1():
    global s

    if s is not None:
        return s

    url = "https://uat2oboe.ef.com/oboe2/login"
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml"
    }
    s = requests.session()
    s.post(url, "username=etown.juno&fakepassword=&password=mail%40123&submit=Log+In",
           headers=headers, verify=False)

    return s