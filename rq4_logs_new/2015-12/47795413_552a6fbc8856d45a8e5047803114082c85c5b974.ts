
var lastTime = 0,
isLittleEndian = true;

export var URL = window.URL || window.webkitURL;

export var requestAnimationFrame = function(callback, element) {
    var requestAnimationFrame =
        window.requestAnimationFrame        ||
        window.webkitRequestAnimationFrame  ||
        window.mozRequestAnimationFrame     ||
        window.oRequestAnimationFrame       ||
        window.msRequestAnimationFrame      ||
        function(callback, element) {
            var currTime = new Date().getTime();
            var timeToCall = Math.max(0, 16 - (currTime - lastTime));
            var id = window.setTimeout(function() {
                callback(currTime + timeToCall);
            }, timeToCall);
            lastTime = currTime + timeToCall;
            return id;
        };

    return requestAnimationFrame.call(window, callback, element);
};

export var cancelAnimationFrame = function(id) {
    var cancelAnimationFrame = window.cancelAnimationFrame ||
                                function(id) {
                                    clearTimeout(id);
                                };
    return cancelAnimationFrame.call(window, id);
};

export var getUserMedia = function(options, success, error) {
    var getUserMedia =
        window.navigator.getUserMedia ||
        window.navigator.mozGetUserMedia ||
        window.navigator.webkitGetUserMedia ||
        window.navigator.msGetUserMedia ||
        function(options, success, error) {
            error();
        };

    return getUserMedia.call(window.navigator, options, success, error);
};

export var detectEndian = function() {
    var buf = new ArrayBuffer(8);
    var data = new Uint32Array(buf);
    data[0] = 0xff000000;
    isLittleEndian = true;
    if (buf[0] === 0xff) {
        isLittleEndian = false;
    }
    return isLittleEndian;
};