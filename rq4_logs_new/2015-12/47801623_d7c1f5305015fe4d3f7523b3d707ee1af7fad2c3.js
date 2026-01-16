/**
 * Created by weichunhe on 2015/12/11.
 */
var http = require('http');
var querystring = require('querystring');
var opts = {
    host: '10.5.6.207',
    port: 9966,
    method: 'POST',
    path: '/graph/history',
    headers: {
        'Content-Type': 'application/json'
    }
};

setInterval(function () {
    var nowSecond = Math.floor(new Date().getTime() / 1000);
    var data = {
        "start": nowSecond - 3600,
        "end": nowSecond,
        "cf": "AVERAGE",
        "endpoint_counters": [
            {
                "endpoint": "test.ep3",
                "counter": "test.counter"
                //,step:10
                //"counter": "load.5min"
            }
        ]
    };
    var req = http.request(opts, function (res) {
        var chunk = '';
        res.on('data', function (data) {
            chunk += data;
        });
        res.on('end', function () {
            console.log('resp:', new Date(), chunk);
        });

    });
    req.write(JSON.stringify(data));
    req.end();
}, 60000);