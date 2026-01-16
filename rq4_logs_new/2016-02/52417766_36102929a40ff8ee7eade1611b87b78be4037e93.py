from sense_hat import SenseHat
from twython import TwythonStreamer
sense = SenseHat()
sense.clear()
app_key = 'appkey'
app_secret = 'appsecret'
access_token = 'accesstoken'
access_token_secret = 'accesstokensecret'

class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            color = data['text'].encode('utf-8')
            print(color)
            if color[13:18] == 'flash':
                color = color[19:len(color)]
                sense.clear()
                sense.show_message(color)

            color = color[13:len(color)].lower()
            if color == 'red':
                print('cherry')
                sense.clear([255, 0, 0])
            elif color == 'orange':
                print('mango')
                sense.clear([255, 127, 0])
            elif color == 'yellow':
                print('pineapple')
                sense.clear([255, 255, 0])
            elif color == 'green':
                print('key lime')
                sense.clear([0, 255, 0])
            elif color == 'blue':
                print('blueberry')
                sense.clear([0, 0, 255])
            elif color == 'indigo':
                print('blackberry')
                sense.clear([75, 0, 130])
            elif color == 'violet':
                print('passion fruit')
                sense.clear([127, 0, 255])
            else:
                print("don't know that recipe!")

    def on_error(self, status_code, data):
        print status_code
        self.disconnect()

stream = MyStreamer(app_key, app_secret,
                    access_token, access_token_secret)
stream.statuses.filter(track='@flavorthepi')