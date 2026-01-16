const fs = require("fs");
const Websocket = require('ws')
const text = fs.readFileSync('./UID_Beta.txt').toString('utf-8');
const textline = text.split('\n')
const result = textline.map((text) => text.split(':')[1])
let arrs = new Map()
let counterror = 0;

for (let i = 0; i < result.length; i++) {

    if (i > 1501) {
        break;
    } else {
        console.log(i, result[i])
        let ws = new Websocket('ws://ec2-13-229-148-190.ap-southeast-1.compute.amazonaws.com/client')
        ws.on('error', (err) => {
            ws.terminate();
            counterror += 1;
            console.log(counterror, err)
        })

        ws.on('open', () => {
            arrs.set(result[i], ws)
            setTimeout(() => {
                ws.send(JSON.stringify({ 'action': 'register', 'uuid': result[i].trim() }))
            }, 3000)

            ws.on('message', (message) => {
                const result = JSON.parse(message)
                console.log(i, result);
            })

            ws.on('close', () => {
                console.log(result[i].trim(), 'had close')
            })
        })
    }
}