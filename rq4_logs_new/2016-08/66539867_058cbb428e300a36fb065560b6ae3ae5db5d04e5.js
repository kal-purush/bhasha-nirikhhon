(function() {

  var $clock = document.getElementById('clock');
  var $htmlColour = document.getElementById('htmlColour');

  var pad = function(s) {
    s = '' + s;
    if (s.length == 1) {
      return '0' + s;
    } else {
      return s;
    }
  };

  var loop = function() {
    var date = new Date();

    var hours = date.getHours();
    var mins = date.getMinutes();
    var secs = date.getSeconds();

    var red = Math.round((hours / 23.0) * 255.0);
    var green = Math.round((mins / 59.0) * 255.0);
    var blue = Math.round((secs / 59.0) * 255.0);

    var html = '#' + pad(red.toString(16)) + pad(green.toString(16)) + pad(blue.toString(16));
    html = html.toUpperCase();

    var text = pad(hours) + ':' + pad(mins) + ':' + pad(secs);

    $htmlColour.textContent = html;
    $clock.textContent = text;
    document.body.style.backgroundColor = html;
  };

  var main = function() {
    loop();

    var milliseconds = (new Date()).getMilliseconds();
    var d = 1000 - milliseconds;

    setTimeout(function() {
      setInterval(loop, 1000);
    }, d);
  };

  main();

}());