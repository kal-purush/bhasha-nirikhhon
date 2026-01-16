const {WebSocket} = require('ws');

const wsClientFactory = (id) => {
    const ws = new WebSocket('ws://localhost:8080');

    ws.on('error', console.error);

    ws.on('open', function open() {
        console.log('connected ' + (id) + ' ')

        ws.send(' ' + (id) + ' Hello');
    });

    ws.on('message', function message(data) {
      console.log(' ' + (id) + ' received: %s', data);
    });
}

const wsClient1 = wsClientFactory(1);
const wsClient2 = wsClientFactory(2);
const wsClient3 = wsClientFactory(3);