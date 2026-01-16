from flask import Flask, render_template, request
app = Flask(__name__)



@app.route("/")
def main():

#   for pin in pins:
#      pins[pin]['state'] = GPIO.input(pin)

   templateData = {
#      'pins' : pins
     }

   return render_template('main.html', **templateData)


@app.route("/manual/<changePin>/<action>")
def m_action(changePin, action):

#   changePin = int(changePin)

 #  deviceName = pins[changePin]['name']

   templateData = {
  #    'pins' : pins
   }

   return render_template('main.html', **templateData)