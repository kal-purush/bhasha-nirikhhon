(function () {
  window.adder = function (a, b) {
    return a + b;
  };

  window.addTextToDom = function (text) {
    var body = document.getElementsByTagName('body')[0];
    body.innerHTML += '<p>' + text + '</p>';
  };
})();