this.addEventListener('install', function(event) {
    event.waitUntil(
        caches.open("restaurantMap").then(function(cache) {
            return cache.addAll([
                './',
                  'index.html',
                  'restaurant.html?id=1',
                  'restaurant.html?id=2',
                  'restaurant.html?id=3',
                  'restaurant.html?id=4',
                  'restaurant.html?id=5',
                  'restaurant.html?id=6',
                  'restaurant.html?id=7',
                  'restaurant.html?id=8',
                  'restaurant.html?id=9',
                  'restaurant.html?id=10',
                  './css/styles.css',
                  './css/responsive.css',
                  './js/main.js',
                  './js/dbhelper.js',
                  './js/restaurant_info.js',
                  './data/restaurants.json',
                  './img/1.jpg',
                  './img/2.jpg',
                  './img/3.jpg',
                  './img/4.jpg',
                  './img/5.jpg',
                  './img/6.jpg',
                  './img/7.jpg',
                  './img/8.jpg',
                  './img/9.jpg',
                  './img/10.jpg',
                  './img/icons/apple-touch-icon-120x120-precomposed.png',
                  './img/icons/apple-touch-icon-152x152-precomposed.png',
                  './img/icons/favicon16.png'
            ]);
        })
    );
});

this.addEventListener('activate', function(event) {
    event.waitUntil(
        caches.keys().then(function(cacheNames) {
            return Promise.all(
                cacheNames.filter(function(cacheName) {
                    return cacheName === "restaurantMap";
                }).map(function(cacheName) {
                    return caches.delete(cacheName);
                })
            );
        })
    );
});

this.addEventListener('fetch', (event) => {
    event.respondWith(
        caches.match(event.request).then(response => {
            if (response) {
                return response;
            }
            return fetch(event.request).then(networkResponse => {
                if (networkResponse.status === 404) {
                    return null;
                }
                return caches.open("restaurantMap").then(cache => {
                    cache.put(event.request.url, networkResponse.clone());
                    return networkResponse;
                });
            });
        }).catch(error => {
            console.log(error);
            return;
        })
    );
});