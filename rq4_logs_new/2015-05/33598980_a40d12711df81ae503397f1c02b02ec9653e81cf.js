(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
'use strict';

var Hello = React.createClass({displayName: "Hello",
  render: function() {
    return (
      React.createElement("h1", null, "Hello, ", this.props.name, " World !!!")
    );
  }
});

module.exports = Hello;

},{}],2:[function(require,module,exports){
'use strict';

(function() {
  var Hello = require('./Hello.jsx');

  React.render(
    React.createElement(Hello, {name: "React Component"}),
    document.getElementById('app')
  );
})();

},{"./Hello.jsx":1}]},{},[2]);