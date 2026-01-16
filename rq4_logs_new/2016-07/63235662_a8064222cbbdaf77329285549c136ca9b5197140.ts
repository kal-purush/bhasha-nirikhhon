import { Injectable } from '@angular/core';

declare var bluetoothle: any;
declare var BLECentral: any;

/*
  Generated class for the Central Bluetooth Low Energy provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class CentralBle {
    private central: any;
    peripherals: any;

    init() {
        console.log('Starting central process.');

        var self = this;

        // init the peripherals object
        self.peripherals = {};

        // init ble central
        self.central = BLECentral();

        // debug
        self.central.setDebug(true);

        // on debug
        self.central.onDebug((message) => {
            console.log(message);
        });

        // on subscribe notify
        self.central.onSubscribe(function(response) {
            // notification from server?
            if(response.status === 'subscribedResult') {
                // get encoded data
                var bytes  = bluetoothle.encodedStringToBytes(response.value);
                // get the string
                var string = bluetoothle.bytesToString(bytes);

                console.log('Notify: ' + string);
                console.log('Notify Bytes: ' + bytes);
            }
        });

        // start scanning for peripherals
        setTimeout(() => {
            self.scan();
        }, 2000);
    }

    /**
     * Start scanning for devices
     */
    scan() {
        var self = this;

        console.log('scanning');

        // start scanning
        self.central.scan((response) => {
            // peripheral result?
            if(response.status === 'scanResult') {
                // maximum rssi?
                if(Math.abs(response.rssi) >= Math.abs(self.central.RSSI_MAX)) {
                    console.log('Device too far away...');
                }

                // update peripherals
                self.peripherals = self.handleScan(response);
            }
        }, (response) => {

        });

        setTimeout(() => {
            // stop the scan
            self.central.stopScan((response) => {
                // update peripheral list
                self.updatePeripheralList(self.peripherals);

                // connect to peripheral
                self.connectToPeripherals(self.peripherals);

                setTimeout(() => {
                    self.scan();
                }, 10000);
            });
        }, 2000);

        return this;
    }

    /**
     * Handle scan results
     */
    handleScan(peripheral) {
        var self = this;

        peripheral.rssi = null;

        // peripheral exists?
        if(!(peripheral.name in this.peripherals)) {
            // set peripheral key
            self.peripherals[peripheral.name] = {};

            // set peripheral info
            self.peripherals[peripheral.name].info   = peripheral;
            // set peripheral status
            self.peripherals[peripheral.name].status = 'disconnected';
            // set peripheral timestamp
            self.peripherals[peripheral.name].added  = Date.now();
            // set peripheral expire
            self.peripherals[peripheral.name].expire = Date.now() + (60000 * 5);

            return self.peripherals;
        }

        // peripheral exists?
        if(peripheral.name in self.peripherals) {
            // get the original
            var original = JSON.stringify(self.peripherals[peripheral.name].info);
            // get the recent
            var recent   = JSON.stringify(peripheral);

            // has the same info?
            if(recent === original) {
                // nothing to do
                return self.peripherals;
            } else {
                console.log('Device information updated.');

                // is it expired?
                if(this.peripherals[peripheral.name].expire <= Date.now()) {
                    // remove the peripheral
                    delete self.peripherals[peripheral.name];

                    return self.peripherals;
                }

                // set peripheral info
                self.peripherals[peripheral.name].info   = peripheral;
            }

            return self.peripherals;
        }
    }

    /**
     * Connect to peripherals
     */
    connectToPeripherals(list) {
        var self = this;

        // do we have peripherals?
        if(JSON.stringify(self.peripherals) === '{}') {
            console.log('No peripherals found.');
            return;
        }

        // device length
        var max   = self.objLength(list);
        var index = 0;

        // iterate on each peripherals
        for(var i in list) {
            // connect to peripheral
            (function(i, list, peripherals) {
                self.central.connect(
                list[i].info.address,

                // on success connection / disconnect
                (response) => {
                    // set device status
                    peripherals[i].status = response.status;
                    // set device information
                    peripherals[i].device = response.info;

                    // are we good?
                    if(index === max) {
                        // update device list
                        self.updatePeripheralList(peripherals);
                    }
                },

                // on error processing connection
                (response) => {
                    peripherals[i].status = 'error';

                    // are we good?
                    if(index === max) {
                        // update peripheral list
                        self.updatePeripheralList(peripherals);
                    }
                });

                index = index + 1;
            })(i, list, self.peripherals);
        }
    }

    /**
     * Update peripheral list
     */
    updatePeripheralList(peripherals) {
        // do we have peripherals?
        if(JSON.stringify(peripherals) === '{}') {
            // TODO: tell that there no available peripherals
            console.log('No available peripherals');
            return;
        }

        // get the peripheral template
        // var baseTpl  = peripheralTpl.innerHTML;
        // combined template
        // var combined = '';

        // iterate on each peripherals
        for(var i in peripherals) {
            console.log(peripherals[i]);
            // var tpl = baseTpl;

            // tpl = tpl
            // .replace('{{name}}', peripherals[i].info.address)
            // .replace('{{id}}', peripherals[i].info.address)
            // .replace('{{id}}', peripherals[i].info.address)
            // .replace('{{status}}', peripherals[i].status);

            // combined += tpl;
        }

        // update peripheral container
        // peripheralContainer.innerHTML = combined;

        return;
    }

    write(data) {
        var self = this;

        var message = data;

        console.log(self.peripherals);

        // prompt for message
        // var message = prompt('Enter your message: ');

        // get the address
        // var address         = e.getAttribute('data-id');
        // var address = self.peripherals[0].info.address;
        // get the information
        var information: any;

        // look for that id
        for(var i in self.peripherals) {
            // matched the id?
            // if(self.peripherals[i].info.address === address) {
                // get the information
                information = self.peripherals[i];

                // break;
            // }
        }

        // get the services
        var services = information.device.services;
        // look for our service
        var service: any;

        for(var i in services) {
            var uuid = services[i].uuid;

            if(uuid === '1000') {
                service = services[i];
            }
        }

        message = [
            '12345678901234567890',
            '12345678911234567891',
            '12345678921234567892',
            '12345678931234567893',
            '12345678941234567894',
            '12345678951234567895',
            '12345678961234567896',
            '12345678971234567897',
            '12345678981234567898',
            '12345678991234567899',
            'abcdef',
            'ghijklmnop'
        ].join('');

        // set request params
        var param = {
            'address'           : information.info.address,
            'service'           : service.uuid,
            'characteristic'    : service.characteristics[0].uuid,
            'type'              : 'noResponse',
            'value'             : message
        };

        // write to device
        // central.write(param, function(response) {
        //     log(response);
        // }, function(response) {
        //     log(response);
        // });

        self.central.writeByChunk(param, function(response) {
            console.log('write', response);
        }, function(response) {

        });
    }

    /**
     * Object length helper
     */
    objLength(object) {
        var len = 0;

        for (var i in object) {
            len = len + 1;
        }

        return len;
    }

    /**
     * Stops the ongoing scan
     */
    stopScan() {
        var self = this;

        self.central.stopScan((response) => {
            console.log(response);
            // // update peripheral list
            // self.updatePeripheralList(self.peripherals);

            // // connect to peripheral
            // self.connectToPeripherals(self.peripherals);

            // setTimeout(() => {
            //     self.scan();
            // }, 10000);
        });
    }
}