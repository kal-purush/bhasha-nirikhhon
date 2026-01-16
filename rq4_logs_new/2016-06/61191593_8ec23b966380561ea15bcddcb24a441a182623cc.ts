let CACHE_NAME = 'angular2-books';

// Service Workerインストール時にキャッシュさせたいファイルを記述
let urlsToCache = [
  '/book.png',
  '/paper.png',
  '/pages/angular2/01-1.png',
  '/pages/angular2/01-2.png',
  '/pages/angular2/02-1.png',
  '/pages/angular2/02-2.png',
  '/pages/angular2/03-1.png',
  '/pages/angular2/03-2.png',
  '/pages/angular2/04-1.png',
  '/pages/angular2/04-2.png',
  '/pages/pique/01.jpg',
  '/pages/pique/02.jpg',
  '/pages/pique/03.jpg',
  '/pages/pique/04.jpg',
  '/pages/pique/05.jpg',
  '/pages/pique/06.jpg'
];

// インストール処理のコールバックをセット
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => {
        return cache.addAll(urlsToCache);
      })
  );
});

self.addEventListener('fetch', (event) => {
  event.respondWith(
    caches.match(event.request)
      .then((response) => {
        // キャッシュがあったのでレスポンスを返す
        if (response) {
          return response;
        }

        // 重要：リクエストを clone する。リクエストは Stream なので
        // 一度しか処理できない。ここではキャッシュ用、fetch 用と2回
        // 必要なので、リクエストは clone しないといけない
        let fetchRequest = event.request.clone();

        return fetch(fetchRequest)
          .then((response) => {
            // レスポンスが正しいかをチェック
            if (!response || response.status !== 200 || response.type !== 'basic') {
              return response;
            }

            // 重要：レスポンスを clone する。レスポンスは Stream で
            // ブラウザ用とキャッシュ用の2回必要。なので clone して
            // 2つの Stream があるようにする
            let responseToCache = response.clone();

            caches.open(CACHE_NAME)
              .then((cache) => {
                if (event.request.url.indexOf('browser-sync') < 0) {
                  // browser-sync以外は、とにかくリソースをキャッシュする。
                  cache.put(event.request, responseToCache);
                }
              });

            return response;
          });
      })
  );
});


// self.addEventListener('activate', function(event) {
//   event.waitUntil(
//     caches.keys().then((cacheNames) => {
//       return Promise.all(
//         cacheNames.map(function(cacheName) {
//           if (CACHE_NAME.indexOf(cacheName) === -1) {
//             return caches.delete(CACHE_NAME);
//           }
//         })
//       );
//     })
//   );
// });