let rCache = 'cache-only-v1';
let sourceCache = [
  '/',
  '/css/styles.css',
  '/css/responsive.css',
  'index.html',
  'restaurant.html',
  '/js/main.js',
  '/js/restaurant_info.js',
  '/js/dbhelper.js',
];

for (let i = 1; i < 11; i++) {
  sourceCache.push(`/img/${i}.jpg`);
};
console.log (sourceCache);


//TODO: add files for cache
self.addEventListener('install', function(event) {
  console.log ('I am here!');
  event.waitUntil(
    caches.open(rCache)
    .then(function(cache) {
      console.log ('open cacheRest-t');
      return cache.addAll(sourceCache);
    })
  );
});


//TODO: fetch response
self.addEventListener('fetch', function(event) {
  event.respondWith(
    caches.match(event.request).then(function(response) {
      if (response) return response;
      return fetch(event.request);
    })
  );
});


/*self.addEventListener('fetch', function(event) {*/
  //event.respondWith(
    //caches.match(event.request)
    //.then(function(response) {
      //if (response) {
        //return response;
      //}
      //var fetchRequest = event.request.clone();
      //return fetch(fetchRequest).then(
        //function(response) {
          //// Check if we received a valid response
          //if(!response || response.status !== 200 || response.type !== 'basic') {
            //return response;
          //}
          //var responseToCache = response.clone();
          //caches.open(restaurantCache)
            //.then(function(cache) {
              //cache.put(event.request, responseToCache);
            //});

          //return response;
        //}
      //);
      //})
    //);
/*});*/