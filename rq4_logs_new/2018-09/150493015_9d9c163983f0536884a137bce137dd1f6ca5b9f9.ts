import * as chrome from 'chrome-launcher';
import * as http from 'http';

const LOCAL_KIBANA_UI = 'http://localhost:5601';
let s = setInterval(() => {
    console.log('Checking if Kibana is up');
    http.get({
        host: 'localhost',
        port: 5601,
        path: '/status'
    }, resp => {
        resp.on('data', () => {
           // ignored
        });
        resp.on('end', () => {
            console.log(`Kibana UI launching`);
            clearInterval(s);
            chrome.launch({
                startingUrl: LOCAL_KIBANA_UI
            }).then(() => {
                console.log(`Kibana UI launched`);
            });
        });
    })
        .on('error', err => console.log(`Not yet!`)).end();
}, 5000);
