importScripts("/assets/desktop/precache-manifest.7344e96b9ec6baafda00a5130f45f8e6.js", "https://storage.googleapis.com/workbox-cdn/releases/3.6.3/workbox-sw.js");

/* eslint-disable no-restricted-globals */
if (workbox) {
  // precaching strategy, if assets are in cache, serve them, without waiting for service worker to install
  workbox.skipWaiting();

  // sync service-workers if opened in simulatenous tabs
  workbox.clientsClaim();

  workbox.routing.registerRoute(/(.+)\?utm_source=homescreen$/, async ({ event }) => {
    try {
      return await workbox.strategies.networkOnly().handle({ event });
    } catch (error) {
      return caches.match('/offline.html');
    }
  });

  workbox.routing.registerRoute(/\/$/, async ({ event }) => {
    try {
      return await workbox.strategies.networkOnly().handle({ event });
    } catch (error) {
      return caches.match('/offline.html');
    }
  });

  // precaching the assets
  // eslint-disable-next-line
  workbox.precaching.precacheAndRoute(
    // eslint-disable-next-line no-underscore-dangle
    (self.__precacheManifest || []).filter(e => {
      if (
        e &&
        e.url &&
        e.url.match &&
        e.url.match(/((login|home|detail|listing)(.+).(js|css)|(offline.html|manifest.json))/)
      ) {
        return true;
      }
      return false;
    }),
    {
      directoryIndex: null,
      cleanUrls: false
    }
  );

  // cache seo api call
  workbox.routing.registerRoute(
    /api\/pwa\/getSeoContent(.*)/,
    workbox.strategies.staleWhileRevalidate({
      cacheName: 'api-cache',
      plugins: [
        new workbox.expiration.Plugin({
          maxEntries: 5,
          maxAgeSeconds: 18000 // 5 Hours
        })
      ]
    })
  );

  // cache font files
  workbox.routing.registerRoute(
    /\.(?:woff|woff2|ttf)$/,
    workbox.strategies.cacheFirst({
      cacheName: 'font-cache',
      plugins: [
        new workbox.expiration.Plugin({
          maxEntries: 10,
          maxAgeSeconds: 30 * 24 * 60 * 60, // 30 Days
          purgeOnQuotaError: true
        }),
        // this enables caching 3rd party response. Beware : if incorrect resource is cached, it won't be updated till it expires
        new workbox.cacheableResponse.Plugin({
          statuses: [0, 200]
        })
      ]
    })
  );
} else {
  console.log(`Boo! Workbox didn't load 😬`);
}
importScripts('https://cdn.moengage.com/webpush/releases/serviceworker_cdn.min.latest.js');
