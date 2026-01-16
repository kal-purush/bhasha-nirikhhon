// lib/raspberryio.js

'use strict';

var gpio = require('rpi-gpio');

gpio.setup(7, gpio.DIR_OUT, enableLights);

function enableLights() {
    //gpio.write(7, true, function(err) {
    //    if (err) throw err;
    //    console.log('Enabled lights');
    //});

    console.log('Enabled lights');

}

exports.enableLights = enableLights;