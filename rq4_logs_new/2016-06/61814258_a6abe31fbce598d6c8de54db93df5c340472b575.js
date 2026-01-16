'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _radon = require('../radon');

var _radon2 = _interopRequireDefault(_radon);

var _settings = require('../settings');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Button = function () {
  function Button() {
    var color = arguments.length <= 0 || arguments[0] === undefined ? 'black' : arguments[0];
    var size = arguments.length <= 1 || arguments[1] === undefined ? 'medium' : arguments[1];

    _classCallCheck(this, Button);

    this['fontFamily'] = 'inherit';
    this['fontSize'] = 'inherit';
    this['textDecoration'] = 'none';
    this['cursor'] = 'pointer';
    this['display'] = 'inline-block';
    this['borderRadius'] = '999rem';
    this['verticalAlign'] = 'middle';
    this['appearance'] = 'none';
    this['height'] = 'auto';
    this['backgroundColor'] = _settings.colors[color];
    this['color'] = _settings.colors['white'];
    this['margin'] = 0;
    this['border'] = 0;
    this['opacity'] = 1;
    this['padding'] = this.handleSizing(size).padding;
    this['lineHeight'] = this.handleSizing(size).lineHeight;
    this['transition'] = 'all .15s ease';
    this[':hover'] = { opacity: '.75' };
    this[':focus'] = { boxShadow: '0 0 0 3px rgba(0,0,0,.3)', outline: 'none' };
  }

  _createClass(Button, [{
    key: 'handleSizing',
    value: function handleSizing(size) {
      switch (size) {
        case 'large':
          return {
            padding: '1.5rem',
            lineHeight: '1.5rem'
          };
          break;
        case 'small':
          return {
            padding: '.75rem',
            lineHeight: '.75rem'
          };
          break;
        default:
          return {
            padding: '1rem',
            lineHeight: '1rem'
          };
          break;
      }
    }
  }]);

  return Button;
}();

exports.default = Button;