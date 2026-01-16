/// <reference path="../typings/tsd.d.ts" />
require('./main.css');

import React = require('react');
import AppView = require('./AppView/AppView');
import TestView = require('./TestView/TestView');

var route = function () {
  console.log(location.hash);
  if (location.hash === '#test') {
    require.ensure(['./TestView/TestView'], (require) => {
      var Test: typeof TestView = require('./TestView/TestView');
      React.render(React.createElement(Test), document.getElementById('app'));
    });
  } else {
    require.ensure(['./AppView/AppView'], (require) => {
      var Home: typeof AppView = require('./AppView/AppView');
      React.render(React.createElement(Home), document.getElementById('app'));
    });
  }
};

window.onhashchange = route;

route();
/*
if (module.hot) {
  module.hot.accept(function () {
    route();
  });
}
*/