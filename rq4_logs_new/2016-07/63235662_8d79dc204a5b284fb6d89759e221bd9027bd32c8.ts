import { Injectable } from '@angular/core';

declare var bluetoothle: any;
declare var BLECentral: any;

/*
  Generated class for the PouchService provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class CentralBle {
    private central: any;
    private peripherals: any;

    init() {
        console.log('Starting central process.');

        // init ble central
        this.central = BLECentral();

        // debug
        this.central.setDebug(true);

        // on debug
        this.central.onDebug((message) => {
            console.log(message);
        });

        // on subscribe notify
        this.central.onSubscribe((response) => {
            // notification from server?
            if(response.status === 'subscribedResult') {
                // get encoded data
                var bytes  = bluetoothle.encodedStringToBytes(response.value);
                // get the string
                var string = bluetoothle.bytesToString(bytes);

                console.log('Notify: ' + string);
            }
        });

        // start scanning for peripherals
        setTimeout(() => {
            this.scan();
        }, 2000);
    }

    /**
     * Start scanning for devices
     */
    scan() {
        // start scanning
        this.central.scan((response) => {
            // peripheral result?
            if(response.status === 'scanResult') {
                // maximum rssi?
                if(Math.abs(response.rssi) >= Math.abs(this.central.RSSI_MAX)) {
                    console.log('Device too far away...');
                }

                // update peripherals
                this.peripherals = this.handleScan(response);
            }
        }, (response) => {

        });

        setTimeout(() => {
            // stop the scan
            this.central.stopScan((response) => {
                // update peripheral list
                this.updatePeripheralList(this.peripherals);

                // connect to peripheral
                this.connectToPeripherals(this.peripherals);

                setTimeout(() => {
                    this.scan();
                }, 10000);
            });
        }, 2000);
    }

    /**
     * Handle scan results
     */
    handleScan(peripheral) {
        peripheral.rssi = null;

        // peripheral exists?
        if(!(peripheral.name in this.peripherals)) {
            // set peripheral key
            this.peripherals[peripheral.name] = {};

            // set peripheral info
            this.peripherals[peripheral.name].info   = peripheral;
            // set peripheral status
            this.peripherals[peripheral.name].status = 'disconnected';
            // set peripheral timestamp
            this.peripherals[peripheral.name].added  = Date.now();
            // set peripheral expire
            this.peripherals[peripheral.name].expire = Date.now() + (60000 * 5);

            return this.peripherals;
        }

        // peripheral exists?
        if(peripheral.name in this.peripherals) {
            // get the original
            var original = JSON.stringify(this.peripherals[peripheral.name].info);
            // get the recent
            var recent   = JSON.stringify(peripheral);

            // has the same info?
            if(recent === original) {
                // nothing to do
                return this.peripherals;
            } else {
                console.log('Device information updated.');

                // is it expired?
                if(this.peripherals[peripheral.name].expire <= Date.now()) {
                    // remove the peripheral
                    delete this.peripherals[peripheral.name];

                    return this.peripherals;
                }

                // set peripheral info
                this.peripherals[peripheral.name].info   = peripheral;
            }

            return this.peripherals;
        }
    }

    /**
     * Connect to peripherals
     */
    connectToPeripherals(list) {
        // do we have peripherals?
        if(JSON.stringify(this.peripherals) === '{}') {
            console.log('No peripherals found.');

            return this;
        }

        // device length
        var max   = this.objLength(list);
        var index = 0;

        // iterate on each peripherals
        for(var i in list) {
            // connect to peripheral
            (function(i, list, peripherals) {
                this.central.connect(
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
                        this.updatePeripheralList(peripherals);
                    }
                },

                // on error processing connection
                (response) => {
                    peripherals[i].status = 'error';

                    // are we good?
                    if(index === max) {
                        // update peripheral list
                        this.updatePeripheralList(peripherals);
                    }
                });

                index = index + 1;
            })(i, list, this.peripherals);
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
            return this;
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

        return this;
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
}