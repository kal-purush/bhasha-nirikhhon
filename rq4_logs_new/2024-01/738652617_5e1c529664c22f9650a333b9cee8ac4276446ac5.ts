import noble from 'noble'

noble.on('stateChange', state => {
    if (state === 'poweredOn') {
        noble.startScanning()
    } else {
        noble.stopScanning()
    }
});

noble.on('discover', peripheral => {
    console.log(peripheral.advertisement.localName);
    // Connect to React Native app
    if (peripheral.advertisement.localName === 'SmartGlass') {
        noble.stopScanning();
        peripheral.connect(error => {
            if (error) {
                console.error(error);
                return;
            }
            console.log('Connected to SmartGlass');
            peripheral.discoverServices([ 'ffe5' ], (error, services) => {
                if (error) {
                    console.error(error);
                    return;
                }
                const service = services[ 0 ];
                service.discoverCharacteristics([ 'ffe9' ], (error, characteristics) => {
                    if (error) {
                        console.error(error);
                        return;
                    }
                    const characteristic = characteristics[ 0 ];
                    characteristic.subscribe(error => {
                        if (error) {
                            console.error(error);
                            return;
                        }
                        console.log('Subscribed for notifications');
                        characteristic.on('data', data => {
                            console.log(data.toString('hex'));
                        })
                    })
                })
            })
        })
    }
})
