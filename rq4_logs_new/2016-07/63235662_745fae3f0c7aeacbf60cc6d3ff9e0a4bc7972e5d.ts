import { Injectable } from '@angular/core';

declare var bluetoothle: any;
declare var BLEPeripheral: any;

/*
  Generated class for the Peripheral Bluetooth Low Energy provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class PeripheralBle {
    private central: any;
    private peripheral: any;

    init() {
        console.log('Starting peripheral process.');

        // init ble peripheral
        this.peripheral = BLEPeripheral();

        // set debug
        this.peripheral.setDebug(true);

        // init advertise
        var advertise = setInterval(() => {
            this.advertise();
        }, 5000);

        // on debug
        this.peripheral.onDebug((message) => {
            // log message
            console.log(message);
        });

        // on peripheral callback
        this.peripheral.onInitPeripheral((response) => {
            // if we are connected
            if(response.status === 'connected') {
                this.central = response;

                // central device name set?
                response.name = response.name ? response.name : response.address;

                // get response
                this.central = response;

                // update central list
                this.updateCentralList(this.central);

                // stop advertising this device
                clearInterval(advertise);

                return;
            }

            // write request?
            if(response.status === 'writeRequested') {
                // get encoded data
                var bytes  = bluetoothle.encodedStringToBytes(response.value);
                // get the string
                var string = bluetoothle.bytesToString(bytes);

                console.log('Write: ' + string);
                console.log('Write Bytes: ' + bytes);
            }

            // subscription?
            if(response.status === 'subscribed') {
                // set subscription data
                this.central.subscribe = response;

                console.log(this.central.address + ' has been subscribed.');
            }


            // disconnection?
            if(response.status === 'disconnected') {
                this.central = {};

                // update list
                this.updateCentralList(this.central);

                // update status
                this.updatePeripheralStatus({});

                // restart interval
                clearInterval(advertise);

                // start interval
                advertise = setInterval(() => {
                    this.advertise();
                }, 10000);

                return;
            }
        });
    }

    /**
     * Start advertising
     */
    advertise() {
        // start advertising
        this.peripheral.advertise((response) => {
            // advertising started?
            if(response.status === 'advertisingStarted') {
                // update peripheral information
                this.updatePeripheralStatus(response);
            }
        }, (response) => {
            console.log('Error occur while advertising device: ' + response.message);
        });
    }

    // notify() {
    //     // set message
    //     var message = prompt('Enter your message: ');

    //     // set request params
    //     var param = {
    //         'address'           : central.address,
    //         'service'           : central.subscribe.service,
    //         'characteristic'    : central.subscribe.characteristic,
    //         'value'             : message
    //     };

    //     // send notify request
    //     for(var i = 0; i < 5; i ++) {
    //         peripheral.notify(param, function(response) {
    //             log(response);
    //         }, function(response) {
    //             log(response);
    //         });
    //     }
    // }

    /**
     * Update central list
     */
    updateCentralList(central) {
        // do we have central?
        if(JSON.stringify(central) === '{}') {
            // no central device
            console.log('No available central device');
            return this;
        }

        // details of the central thingy
        console.log(central);

        // get the central template
        // var baseTpl  = centralTpl.innerHTML;
        // var tpl      = baseTpl;

        // tpl = tpl
        // .replace('{{name}}', central.name)
        // .replace('{{id}}', central.address)
        // .replace('{{id}}', central.address)
        // .replace('{{status}}', central.status);

        // // update central container
        // centralContainer.innerHTML = tpl;

        return this;
    }

    /**
     * Update peripheral status
     */
    updatePeripheralStatus(data) {
        // do we have a status?
        if(JSON.stringify(data) === '{}') {
            // no peripheral status

            return;
        }

        // peripheral status
        console.log(data);

        // // get the template
        // var tpl = statusTpl.innerHTML;

        // tpl = tpl
        // .replace('{{mode}}', data.mode)
        // .replace('{{timeout}}', data.timeout)
        // .replace('{{tx}}', data.txPowerLevel)
        // .replace('{{connectable}}', data.isConnectable)
        // .replace('{{status}}', data.status);

        // peripheralInfo.innerHTML = tpl;
    }
}