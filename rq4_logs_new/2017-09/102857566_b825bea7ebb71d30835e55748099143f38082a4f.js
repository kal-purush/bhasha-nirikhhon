'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Pointer = function () {
  function Pointer(color, x, y) {
    _classCallCheck(this, Pointer);

    this.color = color;
    this.x = x;
    this.y = y;

    this.paint();
  }

  _createClass(Pointer, [{
    key: 'paint',
    value: function paint() {
      var pointer = document.createElement('div');
      this.node = pointer;

      pointer.classList.add('pointer');
      pointer.style.transform = 'translate3d(' + (this.x - 10) + 'px, ' + (this.y - 10) + 'px, 0px)';
      document.body.appendChild(pointer);
    }
  }, {
    key: 'destroy',
    value: function destroy() {
      document.body.removeChild(this.node);
    }
  }, {
    key: 'move',
    value: function move(x, y) {
      this.x = x;
      this.y = y;
      this.node.style.transform = 'translate3d(' + (this.x - 10) + 'px, ' + (this.y - 10) + 'px, 0px)';
    }
  }, {
    key: 'hide',
    value: function hide() {
      this.node.style.display = 'none';
    }
  }, {
    key: 'show',
    value: function show() {
      this.node.style.display = 'block';
    }
  }]);

  return Pointer;
}();