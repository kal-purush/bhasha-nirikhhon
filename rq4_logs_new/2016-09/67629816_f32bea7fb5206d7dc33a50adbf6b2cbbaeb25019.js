(function() {
    var is_defined = typeof _ !== typeof undefined ? true : false;
    if (is_defined) {
        var underscorejs = 'http://underscorejs.org/underscore-min.js';
        // download and run the script
        var head = document.getElementsByTagName('head')[0];
        var script = document.createElement('script');
        script.type = 'text/javascript';
        script.src = underscorejs;
        head.appendChild(script);
    }

})();
_.random(0, 100);