const readline = require('readline');

rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

var dict = new Map();
var lookup = false;
var keys = [];

rl.on('line', line => {
    if (!lookup) {
        if (line == '') {
            lookup = true;
            keys = [...dict.keys()];
            return;
        } else {
            var kv = line.split(' ');
            dict.set(kv[1], kv[0]);
        }
    } else {
        if (keys.includes(line)) {
            console.log(dict.get(line));
        } else {
            console.log("eh");
        }

    }
    
});