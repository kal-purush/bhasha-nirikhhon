(function(f){if(typeof exports==="object"&&typeof module!=="undefined"){module.exports=f()}else if(typeof define==="function"&&define.amd){define([],f)}else{var g;if(typeof window!=="undefined"){g=window}else if(typeof global!=="undefined"){g=global}else if(typeof self!=="undefined"){g=self}else{g=this}g.havanaError = f()}})(function(){var define,module,exports;return (function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _ = new WeakMap();

var Error = (function () {
  function Error(config) {
    _classCallCheck(this, Error);

    var props = {
      'event': config.event,
      'name': 'error',
      'reporting': config.reporting
    };

    _.set(this, props);

    this.init();
  }

  _createClass(Error, [{
    key: 'init',
    value: function init() {
      var _$get = _.get(this);

      var event = _$get.event;
      var name = _$get.name;
      var reporting = _$get.reporting;

      event.subscribe('response.error', function (data) {
        if (reporting.level > 0) {
          reporting.reporter('-- Response sent from handler: error');
        }

        event.publish('response.send', {
          'name': name,
          'id': data.id,
          'time': Date.now(),
          'statusCode': 404
        });
      });
    }
  }]);

  return Error;
})();

exports['default'] = Error;
module.exports = exports['default'];

},{}]},{},[1])(1)
});