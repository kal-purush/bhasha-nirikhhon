self.addEventListener('fetch', e => {
  console.log(e.request);
  e.respondWith(
    caches.match(e.request).then(response => {
      if (response) {
        return response;
      }
      return fetch(e.request);
    })
  );
});
self.addEventListener('install', e => {
  console.log('installed');
  const myCache = ['REST_CACHE-1'];
  const urlsToCache = [
    '/',
    '/index.html',
    '/restaurant.html',
    'restaurant.html?id=*',
    '/css/styles.css',
    '/js/main.js',
    '/js/restaurant_info.js',
    '/img/*.jpg',
  ];
  e.waitUntil(
    caches
      .open(myCache[myCache.length - 1])
      .then(cache => cache.addAll(urlsToCache))
  );
});
self.addEventListener('activate', e => {
  console.log('activated');
});