self.addEventListener('fetch', function(event) {
    console.log(new Date() + ' : ' + event.request.url);
    if (event.request.url.indexOf('test') != -1) {
      event.respondWith(new Response(
          '<img src="./test.png">',
          {headers: [['Content-Type', 'text/html']]}));
    }
  });