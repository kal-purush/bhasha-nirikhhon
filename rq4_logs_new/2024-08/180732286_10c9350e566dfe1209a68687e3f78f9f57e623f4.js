self.addEventListener('install', function(event) {
    console.log('Service Worker 安装中');
});

self.addEventListener('activate', function(event) {
    console.log('Service Worker 激活');
});

self.addEventListener('fetch', function(event) {
    console.log('拦截请求:', event.request.url);
});