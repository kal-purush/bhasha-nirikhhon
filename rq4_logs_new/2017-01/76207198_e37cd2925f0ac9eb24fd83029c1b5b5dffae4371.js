var CACHE = 'V1';

self.addEventListener('install', function(evt) {
  console.log('The service worker is being installed.');
  evt.waitUntil(precache());
});

self.addEventListener('fetch', function(evt) {
  evt.respondWith(fromCache(evt.request).catch(function () {
    return fromNetwork(evt.request, 10000);
  }));
});

function cacheCrossDomain (cache, url) {
  var req = new Request(url, {mode: 'no-cors'});
  return fetch(req)
    .then(res => cache.put(req, res));
}

function precache() {
  return caches.open(CACHE).then(function (cache) {
    return cache.addAll([
      '/',
      './index.html',
      './css/app.css',
      './images/monzo-logo.png',
      './js/compiled/app.js'
    ])
      .then(() => {
	return cacheCrossDomain(cache, 'https://fonts.googleapis.com/icon?family=Material+Icons');
      })
      .then(() => {
	return cacheCrossDomain(cache, 'https://code.getmdl.io/1.2.1/material.indigo-pink.min.css');
      });
  });
}

function fromNetwork(request, timeout) {
  return new Promise(function (fulfill, reject) {
    // Reject in case of timeout.
    var timeoutId = setTimeout(reject, timeout);
    // Fulfill in case of success.
    fetch(request).then(function (response) {
      clearTimeout(timeoutId);
      fulfill(response);
    // Reject also if network fetch rejects.
    }, reject);
  });
}

function fromCache(request) {
  return caches.open(CACHE).then(function (cache) {
    return cache.match(request).then(function (matching) {
      return matching || Promise.reject('no-match');
    });
  });
}