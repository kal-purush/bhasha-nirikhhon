import requests
from datetime import datetime
import smtplib
import time

MY_EMAIL = 'philip.gowland@gmail.com'
PASSWORD = 'vgcneufnfkyixtni'
MY_LAT = 54.9778
MY_LONG = -1.6129
MY_POSITION = (MY_LONG, MY_LAT)


def ISS_nearby():
    ISS_request = requests.get(url='http://api.open-notify.org/iss-now.json')
    ISS_request.raise_for_status()
    ISS_data = ISS_request.json()
    ISS_longitude = float(ISS_data['iss_position']['longitude'])
    ISS_latitude = float(ISS_data['iss_position']['latitude'])
    if MY_LAT - 5 < ISS_latitude < MY_LAT + 5 and MY_LONG - 5 < ISS_longitude < MY_LONG + 5:
        return True


def currently_dark():
    sunrise_parameters = {
        'lat': MY_LAT,
        'lng': MY_LONG,
        'formatted': 0,
    }
    sunrise_request = requests.get(url="https://api.sunrise-sunset.org/json", params=sunrise_parameters)
    sunrise_request.raise_for_status()
    sunrise_data = sunrise_request.json()
    sunrise_time = int(sunrise_data['results']['sunrise'].split("T")[1].split(":")[0])
    sunset_time = int(sunrise_data['results']['sunset'].split("T")[1].split(":")[0])
    time_now = datetime.now().hour
    if time_now <= sunrise_time or time_now >= sunset_time:
        return True

while True:
    if currently_dark() and ISS_nearby():
        with smtplib.SMTP("smpt.gmail.com") as connection:
            time.sleep(60)
            connection.starttls()
            connection.login(user=MY_EMAIL, password=PASSWORD)
            connection.sendmail(
                from_addr=MY_EMAIL,
                to_addrs=MY_EMAIL,
                msg="Subject:International Space Station\n\nThe ISS is overhead somewhere. Look up."
            )