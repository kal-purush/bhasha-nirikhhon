interface JsonpRequestParam {
  url : string;
  data: any;  
  success : (res: any) => void;
  error: (evt: any) => void;
}
var nanoJsonp = function(params: JsonpRequestParam) {
  params.data = params.data || {};
  var computedUrl = function(p: {url: string; data: any}) {
      return p.url + (p.url.indexOf('?') < 0 ? '?' : '&') + objectToURI(p.data);
  };

  var objectToURI = (obj) => {
    var data = [];
    for (var key in obj) data.push(encodeURIComponent(key) + '=' + encodeURIComponent(obj[key]));
    return data.join('&');
  };
  var done = false;
  var callback = params.data['callback'] = 'jsonp_' + Math.random().toString(36)[2] + '_' + Date.now();// random
  var removeCallback = function() {
    try {
      delete this[callback];
    } catch (_error) {
      this[callback] = void 0;
    }
  };
  this[callback] = (data) => {
    params.success(data);
    removeCallback();
  };
  var s = document.createElement('script');
  s.src = computedUrl(params);
  s.async = true;
  s.onerror = (evt) => {
    params.error(evt);
    removeCallback();
  };
  s.onload = s.onreadystatechange = function() {
    if (!done && (!this.readyState || this.readyState === 'loaded' || this.readyState === 'complete')) {
      done = true;
      s.onload = s.onreadystatechange = null;
      if (s.parentNode) s.parentNode.removeChild(s);
      s = null;
    }
  };
  var h = document.querySelector('head') || document.documentElement;
  h.insertBefore(s, h.firstChild);
};