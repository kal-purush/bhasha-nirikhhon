'use strict'
'use strict';

(function () {
  var SERVER_URL = 'https://js.dump.academy/kekstagram';
  var SERVER_TIMEOUT = 5000;
  function save(data, onLoad, onError) {
    var xhr = new XMLHttpRequest();
    xhr.responseType = 'json';
    xhr.timeout = SERVER_TIMEOUT;
    xhr.addEventListener('load', function () {
      if (xhr.status === Status.OK) {
        onLoad('Данные успешно отправлены');
      } else {
        onError('Неизвестный статус: ' + xhr.status + ' ' + xhr.statusText);
      }
    });

    xhr.open('POST', SERVER_URL);
    xhr.send(data);
  }
  window.backend = {
    save: save,
  };
})();