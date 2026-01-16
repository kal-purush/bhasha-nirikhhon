const DEBUG = 0;
let ll;

function logElementEvent(eventName, element) {
  if (DEBUG)
    console.log(new Date().getTime(), eventName, element.getAttribute('data-srcset'));
}

window.lazyLoadOptions = {
  callback_load: function (element) {
    logElementEvent("LOADED", element);
  },
  callback_set: function (element) {
    logElementEvent("SET", element);
  }
};

window.addEventListener('LazyLoad::Initialized', function (e) {
  ll = e.detail.instance;
  ll.update();
}, false);

(function (w, d) {
  var b = d.getElementsByTagName('body')[0];
  var s = d.createElement("script");
  s.async = true;
  var v = !("IntersectionObserver" in w) ? "8.5.2" : "10.3.5";
  s.src = "https://cdnjs.cloudflare.com/ajax/libs/vanilla-lazyload/" + v + "/lazyload.min.js";
  w.lazyLoadOptions = {}; // Your options here. See "recipes" for more information aboyt async.
  b.appendChild(s);
}(window, document));