'use-strict';

var mac = require('./util/mac');

var debug = require('debug')('camera-ffmpeg-ufv');

var object = {};

object.createService = function (hap, nvrConfig, discoveredCamera, cache) {
    // var Accessory = hap.Accessory;
    var Service = hap.Service;
    var Characteristic = hap.Characteristic;
    var UUIDGen = hap.uuid;

    var MotionSensor = {
        timers: [],
        getMotionDetected: function () {
            var val = 0; // 0 = motion not detected
            cache.map(function(recording){
                if (recording.eventType == "motionRecording" && recording.cameras.indexOf(discoveredCamera._id) > -1) {
                    val = 1;
                }
            });
            val = Boolean(val);
            val = Number(val);
            return val;
        },

        getStatusActive: function () {
            // TODO: Armed
            var val = 1;
            val = Boolean(val);
            val = Number(val);
            return val;
        }
    };

    var name = discoveredCamera.name + " Motion Sensor";

    var service = new Service.MotionSensor(name);

    service
        .getCharacteristic(Characteristic.MotionDetected)
        .on('get', function (callback) {
            var err = null;
            callback(err, MotionSensor.getMotionDetected());
        });

    // Update Loop, regular polling for updates.
    MotionSensor.timers.push(setInterval(function () {
        var value = MotionSensor.getMotionDetected();
        if (value != service.getCharacteristic(Characteristic.MotionDetected).value) {
            service.setCharacteristic(Characteristic.MotionDetected, value);
            debug(service.name + " motion sensor state change: " + value);
        }
    }, 1000));

    service.destroy = function() {
        for (var i in MotionSensor.timers) {
            clearInterval(MotionSensor.timers[i]);
        }
        MotionSensor.timers.length = 0;
    }

    return service;
};

module.exports = object;