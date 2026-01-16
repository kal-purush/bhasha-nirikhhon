const https = require('https');
const authKey = '235391AG8E7NoOgG5b8d4843';

function sendSMS(mobile, template) {
    // var options = {
    //     "method": "GET",
    //     "hostname": "api.msg91.com",
    //     "port": null,
    //     "path": encodeURIComponent(``),
    //     "headers": {}
    // };

    // var req = http.request(options, function (res) {
    //     var chunks = [];
    //     res.on("data", function (chunk) {
    //         console.log('chunk', chunk);
    //         chunks.push(chunk);
    //     });

    //     res.on("end", function () {
    //         var body = Buffer.concat(chunks);
    //         console.log(body.toString());
    //     });
    // });

    // req.end();


    https.get(encodeURI(`https://api.msg91.com/api/sendhttp.php?mobiles=91${mobile}&authkey=${authKey}&route=4&sender=PROCKT&message=${template}&country=91`), (resp) => {
        let data = '';

        // A chunk of data has been recieved.
        resp.on('data', (chunk) => {
            data += chunk;
            console.log('data', data)
        });
    }).on("error", (err) => {
        console.log("Error: " + err.message);
    });
}

module.exports = sendSMS;