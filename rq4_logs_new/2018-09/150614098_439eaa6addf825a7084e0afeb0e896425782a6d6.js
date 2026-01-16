/**
 * Usage: phantomjs photo.js http(s)://www.url.tld/
 */

var page = require('webpage').create(),
    system = require('system');

var url = system.args[1],
    stamp = new Date().getTime(),
    bytes = Math.floor((1 + Math.random()) * 0x100000000).toString(16).substring(1);

page.viewportSize = { width: 1600, height: 1200 };
page.open(url);

page.onError = function(msg, trace) {
    var msgStack = ['ERROR: ' + msg];
    if (trace && trace.length) {
        msgStack.push('TRACE:');
        trace.forEach(function(t) {
            msgStack.push(' -> ' + t.file + ': ' + t.line + (t.function ? ' (in function "' + t.function + '")' : ''));
        });
    }
    // uncomment to log into the console 
    // console.error(msgStack.join('\n'));
};

page.onLoadFinished = function() {

    var real_url = page.url;
    var uid = stamp + '_' + bytes;

    page.evaluate(function() {
        var div = document.createElement('div');
        div.innerHTML = document.URL;
        div.style.background='#fff';
        div.style.color='#000';
        div.style.padding='5px';
        div.style.fontWeight='bolder';
        div.style.textAlign="center";
        div.style.borderBottom='1px solid #000';
        div.style.position='absolute',
        div.style.top='-30px';
        div.style.width='100%';

        par = document.body;
        par.style.position='absolute';
        par.style.top='30px';
        par.style.width='100%';

        par.insertBefore(div, par.firstChild);
    });

    page.render('results/' + uid + '.png');
    

    var result = '{ "input" : "' + real_url + '", "result" : "results/' + uid + '.png" }';
    console.log(result);
    phantom.exit();
}