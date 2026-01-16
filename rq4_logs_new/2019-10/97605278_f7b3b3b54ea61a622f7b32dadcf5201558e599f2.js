/**
 * Check out https://googlechromelabs.github.io/sw-toolbox/ for
 * more info on how to use sw-toolbox to custom configure your service worker.
 */

importScripts('https://storage.googleapis.com/workbox-cdn/releases/3.6.1/workbox-sw.js');

if (workbox) {
    console.debug("Yay! Workbox is loaded 🎉");
} else {
    console.debug("Boo! Workbox didn't load 😬");
}

// Cache CSS files
workbox.routing.registerRoute(
    /.*\.css/,
    workbox.strategies.staleWhileRevalidate({
        cacheName: 'css-cache'
    })
);

// Cache image files
workbox.routing.registerRoute(
    /.*\.(?:png|jpg|jpeg|svg|gif)/,
    workbox.strategies.staleWhileRevalidate({
        cacheName: 'image-cache'
    })
);

// Cache default
workbox.routing.setDefaultHandler(
    workbox.strategies.networkFirst({
        cacheName: 'default-cache'
    })
);