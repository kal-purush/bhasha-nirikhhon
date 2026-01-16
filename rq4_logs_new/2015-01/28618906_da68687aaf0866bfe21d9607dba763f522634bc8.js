var system = require('system');
var page = require('webpage').create();

var startTime = new Date().getTime();

function checkLoaded() {
    return page.evaluate(function () {
        return document.all["compositionComplete"];
    }) != null;
}
// Open the page
page.open(system.args[1], function () { });

var checkComplete = function () {
    // Check whether all rendering is complete every second, and timeout after 10 seconds
    if (new Date().getTime() - startTime > 10000 || checkLoaded()) {
        clearInterval(checkCompleteInterval);

        var result = page.content;
        console.log(result);
        setTimeout(function () {
            phantom.exit();     // Need to do it this way to prevent security error message from appearing
        }, 0);
    }
}

// Let us check to see if the page is finished rendering
var checkCompleteInterval = setInterval(checkComplete, 1000);