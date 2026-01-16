// Read cookies

(function () {
  var cookies;

  function readCookie(name, c, C, i) {
    if (cookies) {
      return cookies[name];
    }

    c = document.cookie.split('; ');
    cookies = {};

    for (i = c.length - 1; i >= 0; i--) {
      C = c[i].split('=');
      cookies[C[0]] = C[1];
    }

    return cookies[name];
  }

  window.readCookie = readCookie;
})();

var iris = {};

iris.url = decodeURIComponent(readCookie("Drupal.visitor.iris_server"));

var socket = io(iris.url);

socket.on("connect", function () {

  socket.emit('pair', {
    credentials: {
      "userid": readCookie("Drupal.visitor.iris_userid"),
      "token": readCookie("Drupal.visitor.iris_token"),
    }
  });
});

socket.on("pair", function (result) {

  if (result) {

    console.log("Paired with Iris");

  }

})