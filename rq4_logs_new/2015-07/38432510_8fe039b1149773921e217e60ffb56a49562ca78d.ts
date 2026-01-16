import socketManager from '../socket.io/socketManager';

class TemparatureCollector {
    constructor() {
        var Cylon = require('cylon');

        Cylon
            .robot({ name: 'Temperature' })
            .connection('edison', { adaptor: 'intel-iot' })
            .device('sensor', { driver: 'analogSensor', pin: 0, connection: 'edison' })
            .on('ready', function(my) {
                var sensorVal = 0;
                var ready = false;

                my.sensor.on('analogRead', function(data) {
                    ready = true;
                    sensorVal = data;
                    console.log('Temperature Sensor Value:' + sensorVal);
                });

                setInterval(function() {
                    if (ready) {
                        socketManager.sendData({ date: new Date(), type: 'temparature', value: sensorVal });
                    }
                }, 2000);
            })
            .start();
    }
}

export default new TemparatureCollector();