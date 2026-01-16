const https = require("https");
const apps = (process.env.APPS || '').split(',');
awake();
setInterval(function() {
    awake();
}, 300000);

function awake() {
    console.debug('wake up', apps);
    apps.forEach(app => https.get(`https://${app}.herokuapp.com`));
}