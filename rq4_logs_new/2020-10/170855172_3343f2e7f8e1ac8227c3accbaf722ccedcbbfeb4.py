from flask import render_template, request, flash, redirect, url_for
from flask import Flask

app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13db0c676dfde280fe245'

# import RPi.GPIO as GPIO
# import time

# GPIO.setmode(GPIO.BCM) # set the mode to broadcomm
# GPIO.cleanup()

# #choose the GPIO pin
# relay_in1 = 23  # pin 16

# # declare selected pin as output pin
# GPIO.setup(relay_in1, GPIO.OUT)


@app.route('/', methods=['GET', 'POST'])
@app.route('/home', methods=['GET', 'POST'])
def home():
    if "On" in request.form:
        print("On!")
        is_bulb_on = True
        # GPIO.output(relay_in1, GPIO.HIGH)
    elif "Off" in request.form:
        print("Off")
        is_bulb_on = False
        # GPIO.output(relay_in1, GPIO.LOW)
    else:
        is_bulb_on = False
    return render_template("index.html", is_bulb_on=is_bulb_on)


if __name__ == "__main__":
    app.run(debug=True)