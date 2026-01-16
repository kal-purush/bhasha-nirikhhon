
var script = document.createElement('script');
script.src = 'http://code.jquery.com/jquery-1.11.0.min.js';
script.type = 'text/javascript';
document.getElementsByTagName('head')[0].appendChild(script);

window.onscroll = function (e) {
    if (window.pageYOffset > 30) {
        $('nav').addClass('shrink');
    }
    else {
        $('nav').removeClass('shrink');
    }
    var rotateWith = window.pageYOffset / 20; // TODO: transform coordinate system
    document.getElementById('pic').style.transform = "rotate("+rotateWith+"deg)";
};
