const express = require('express');
const ngrok = require("@ngrok/ngrok");

const app = express();
app.get('/fdsfs', (req, res) => {
    res.send('Hello from Ngrok!');
});

;(async () => {
    app.listen(8080, async () => {
        const listener = await ngrok.forward({ addr: 8080, authtoken_from_env: true });
        // Output ngrok url to console
        console.log(`Ingress established at: ${listener.url()}`);
        process.stdin.resume();
    });    
})();
