(function (window) {
  'use strict';

  function createSWMochaStartFn() {
    return function () {
      var iframe = document.createElement('IFRAME');
      iframe.id = 'sw-mocha-iframe';
      iframe.src = '/base/testing/karma-sw-mocha/index.html';
      iframe.setAttribute('hidden', 'hidden');
      document.body.appendChild(iframe);
    };
  }

  window.__karma__.start = createSWMochaStartFn();
}(window));