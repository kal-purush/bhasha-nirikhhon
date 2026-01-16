(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
var Blackbird = require('merlin.js');

var facets = [
  Blackbird.enumFacet({
    field: 'brand_id',
    num: 2000
  }),
  Blackbird.enumFacet({
    field: 'category_id',
    num: 100
  }),
  Blackbird.enumFacet({
    field: 'sizing_id',
    num: 100
  })
];

Blackbird
.engine({
  fq: 'thredup.staging.wrangler'
})
.search({
  q: '',
  facet: facets
})
.end(function (err, res) {
  if (err) { throw err; }
  var url = res.req.url;
  document.write('request url:', decodeURIComponent(url));
});

},{"merlin.js":7}],2:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

exports['default'] = engine;

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { 'default': obj }; }

function _slicedToArray(arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i['return']) _i['return'](); } finally { if (_d) throw _e; } } return _arr; } else { throw new TypeError('Invalid attempt to destructure non-iterable instance'); } }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _superagent = require('superagent');

var _superagent2 = _interopRequireDefault(_superagent);

var _helpers = require('./helpers');

var _request = require('./request');

var Engine = (function () {
  function Engine(options) {
    _classCallCheck(this, Engine);

    this.protocol = options.protocol || 'https';
    this.company = options.company;
    this.environment = options.environment;
    this.instance = options.instance;
    this.fq = options.fq;
    this.typeaheadRateLimit = options.typeaheadRateLimit || 20;

    if (!this.company) {
      throw new Error('The \'company\' parameter is required.');
    }
    if (!this.environment) {
      throw new Error('The \'environment\' parameter is required.');
    }
    if (!this.instance) {
      throw new Error('The \'instance\' parameter is required.');
    }
  }

  _createClass(Engine, [{
    key: 'fq',
    get: function () {
      return '' + this.company + '.' + this.environment + '.' + this.instance;
    },
    set: function (val) {
      var groups = _helpers.RE2.exec(val);
      if (groups) {
        var _groups = _slicedToArray(groups, 4);

        var company = _groups[1];
        var environment = _groups[2];
        var instance = _groups[3];

        this.company = company;
        this.environment = environment;
        this.instance = instance;
      }
    }
  }, {
    key: 'cluster',
    get: function () {
      return 'search-' + this.environment;
    }
  }, {
    key: 'target',
    get: function () {
      return '' + this.protocol + '://' + this.cluster + '.search.blackbird.am/' + this.fq;
    }
  }, {
    key: 'search',
    value: function search(req) {
      return _superagent2['default'].get('' + this.target + '/search').query((0, _request.searchRequest)(req));
    }
  }, {
    key: 'msearch',
    value: function msearch(req) {
      return _superagent2['default'].get('' + this.target + '/msearch').query((0, _request.multiSearchRequest)(req));
    }
  }, {
    key: 'typeahead',
    value: function typeahead(req) {
      return _superagent2['default'].get('' + this.target + '/typeahead').query((0, _request.typeaheadRequest)(req));
    }
  }, {
    key: 'similar',
    value: function similar(req) {
      return _superagent2['default'].get('' + this.target + '/similar').query((0, _request.similarRequest)(req));
    }
  }, {
    key: 'track',
    value: function track(req) {
      var treq = trackRequest(req);
      return _superagent2['default'].get('' + this.target + '/track/' + treq.type).query(treq.query);
    }
  }]);

  return Engine;
})();

function engine(options) {
  return new Engine(options);
}

module.exports = exports['default'];
},{"./helpers":6,"./request":8,"superagent":10}],3:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; desc = parent = getter = undefined; _again = false; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

exports.expression = expression;
exports.andExpression = andExpression;
exports.orExpression = orExpression;

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _helpers = require('./helpers');

var Expression = (function () {
  function Expression(options) {
    _classCallCheck(this, Expression);

    var field = options.field;
    var operator = options.operator;
    var value = options.value;

    if (!field) {
      throw new Error('Expression#field is required.');
    }
    if (!operator) {
      throw new Error('Expression#operator is required.');
    }
    if (!value) {
      throw new Error('Expression#value is required.');
    }
    if (!(0, _helpers.checkConstructor)(field, String)) {
      throw new Error('Expression#field must be a string.');
    }
    if (!_helpers.RE1.test(field)) {
      throw new Error('Expression#field can only contain a-z, 0-9, and underscores, and must start with a lowercase letter.');
    }
    if (!(0, _helpers.checkConstructor)(value, String, Number)) {
      throw new Error('Expression#value must be a string or a number.');
    }
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  _createClass(Expression, [{
    key: 'toString',
    value: function toString() {
      var rhs = undefined,
          op = this.operator,
          val = this.value;
      switch (op) {
        case '=':
          rhs = '' + val;break;
        case '!=':
          rhs = '!' + val;break;
        case '<':
          rhs = '(:' + val + ')';break;
        case '>':
          rhs = '(' + val + ':)';break;
        case '<=':
          rhs = '(:' + val + ']';break;
        case '>=':
          rhs = '[' + val + ':)';break;
        default:
          throw new Error('Expression#operator must be one of the following: =, !=, <, >, <=, or >=.');
      }
      return '' + this.field + ':' + rhs;
    }
  }]);

  return Expression;
})();

var AndExpression = (function (_Expression) {
  function AndExpression(options) {
    _classCallCheck(this, AndExpression);

    _get(Object.getPrototypeOf(AndExpression.prototype), 'constructor', this).call(this, options);
  }

  _inherits(AndExpression, _Expression);

  _createClass(AndExpression, [{
    key: 'toString',
    value: function toString() {
      return ',' + _get(Object.getPrototypeOf(AndExpression.prototype), 'toString', this).call(this);
    }
  }]);

  return AndExpression;
})(Expression);

var OrExpression = (function (_Expression2) {
  function OrExpression(options) {
    _classCallCheck(this, OrExpression);

    _get(Object.getPrototypeOf(OrExpression.prototype), 'constructor', this).call(this, options);
  }

  _inherits(OrExpression, _Expression2);

  _createClass(OrExpression, [{
    key: 'toString',
    value: function toString() {
      return '|' + _get(Object.getPrototypeOf(OrExpression.prototype), 'toString', this).call(this);
    }
  }]);

  return OrExpression;
})(Expression);

function expression(options) {
  return new Expression(options);
}

function andExpression(options) {
  return new AndExpression(options);
}

function orExpression(options) {
  return new OrExpression(options);
}
},{"./helpers":6}],4:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; desc = parent = getter = undefined; _again = false; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

exports.enumFacet = enumFacet;
exports.histFacet = histFacet;
exports.rangeFacet = rangeFacet;

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _helpers = require('./helpers');

var Facet = (function () {
  function Facet(options) {
    _classCallCheck(this, Facet);

    var field = options.field;

    if (!(0, _helpers.checkConstructor)(field, String)) {
      throw new Error('Facet#field must be a string.');
    }
    if (!_helpers.RE1.test(field)) {
      throw new Error('Facet#field can only contain a-z, 0-9, and underscores, and must start with a lowercase letter.');
    }
    this.field = field;
  }

  _createClass(Facet, [{
    key: 'toString',
    value: function toString() {
      return 'field=' + this.field;
    }
  }]);

  return Facet;
})();

var EnumFacet = (function (_Facet) {
  function EnumFacet(options) {
    _classCallCheck(this, EnumFacet);

    _get(Object.getPrototypeOf(EnumFacet.prototype), 'constructor', this).call(this, options);
    this.num = Number(options.num) || 0;
  }

  _inherits(EnumFacet, _Facet);

  _createClass(EnumFacet, [{
    key: 'toString',
    value: function toString() {
      return '' + _get(Object.getPrototypeOf(EnumFacet.prototype), 'toString', this).call(this) + '/type=enum/num=' + this.num;
    }
  }]);

  return EnumFacet;
})(Facet);

var HistFacet = (function (_Facet2) {
  function HistFacet(options) {
    _classCallCheck(this, HistFacet);

    _get(Object.getPrototypeOf(HistFacet.prototype), 'constructor', this).call(this, options);
    this.start = Number(options.start) || 0;
    this.end = Number(options.end) || 0;
    this.gap = Number(options.gap) || 0;
  }

  _inherits(HistFacet, _Facet2);

  _createClass(HistFacet, [{
    key: 'range',
    get: function () {
      return '[' + this.start + ':' + this.end + ':' + this.gap + ']';
    }
  }, {
    key: 'toString',
    value: function toString() {
      return '' + _get(Object.getPrototypeOf(HistFacet.prototype), 'toString', this).call(this) + '/type=hist/range=' + this.range();
    }
  }]);

  return HistFacet;
})(Facet);

var RangeFacet = (function (_Facet3) {
  function RangeFacet(options) {
    _classCallCheck(this, RangeFacet);

    _get(Object.getPrototypeOf(RangeFacet.prototype), 'constructor', this).call(this, options);
  }

  _inherits(RangeFacet, _Facet3);

  _createClass(RangeFacet, [{
    key: 'toString',
    value: function toString() {
      return '' + _get(Object.getPrototypeOf(RangeFacet.prototype), 'toString', this).call(this) + '/type=range';
    }
  }]);

  return RangeFacet;
})(Facet);

function enumFacet(options) {
  return new EnumFacet(options);
}

function histFacet(options) {
  return new HistFacet(options);
}

function rangeFacet(options) {
  return new RangeFacet(options);
}
},{"./helpers":6}],5:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; desc = parent = getter = undefined; _again = false; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

exports.cnfFilter = cnfFilter;
exports.dnfFilter = dnfFilter;

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _helpers = require('./helpers');

var _expression = require('./expression');

var Filter = (function () {
  function Filter(options) {
    _classCallCheck(this, Filter);

    this.expressions = [];
    this.expressions.push((0, _expression.expression)(options));
  }

  _createClass(Filter, [{
    key: 'and',
    value: function and(options) {
      this.expressions.push((0, _expression.andExpression)(options));
      return this;
    }
  }, {
    key: 'or',
    value: function or(options) {
      this.expressions.push((0, _expression.orExpression)(options));
      return this;
    }
  }, {
    key: 'tag',
    value: function tag(input) {
      if (!(0, _helpers.checkConstructor)(input, String)) {
        throw new Error('Filter tags must be strings.');
      }
      if (!_helpers.RE1.test(input)) {
        throw new Error('Filter tags can only contain a-z, 0-9, and underscores, and must start with a lowercase letter.');
      }
      this._tag = input;
      return this;
    }
  }, {
    key: 'tagString',
    get: function () {
      if (this._tag) {
        return '/tag=' + this._tag;
      }
      return '';
    }
  }, {
    key: 'toString',
    value: function toString() {
      var expressions = this.expressions.reduce(function (result, exp) {
        return result + exp.toString();
      }, '');

      return 'exp=' + expressions + '' + this.tagString;
    }
  }]);

  return Filter;
})();

var CnfFilter = (function (_Filter) {
  function CnfFilter(options) {
    _classCallCheck(this, CnfFilter);

    _get(Object.getPrototypeOf(CnfFilter.prototype), 'constructor', this).call(this, options);
  }

  _inherits(CnfFilter, _Filter);

  _createClass(CnfFilter, [{
    key: 'toString',
    value: function toString() {
      return '' + _get(Object.getPrototypeOf(CnfFilter.prototype), 'toString', this).call(this) + '/type=cnf';
    }
  }]);

  return CnfFilter;
})(Filter);

var DnfFilter = (function (_Filter2) {
  function DnfFilter(options) {
    _classCallCheck(this, DnfFilter);

    _get(Object.getPrototypeOf(DnfFilter.prototype), 'constructor', this).call(this, options);
  }

  _inherits(DnfFilter, _Filter2);

  _createClass(DnfFilter, [{
    key: 'toString',
    value: function toString() {
      return '' + _get(Object.getPrototypeOf(DnfFilter.prototype), 'toString', this).call(this) + '/type=dnf';
    }
  }]);

  return DnfFilter;
})(Filter);

function cnfFilter(options) {
  return new CnfFilter(options);
}

function dnfFilter(options) {
  return new DnfFilter(options);
}
},{"./expression":3,"./helpers":6}],6:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});
exports.checkConstructor = checkConstructor;
exports.mSearchSerialize = mSearchSerialize;
exports.set = set;
var _ = {
  isUndefined: function isUndefined(val) {
    return val === undefined;
  },
  isNull: function isNull(val) {
    return val === null;
  },
  isNaN: function isNaN(val) {
    return Number.isNaN(val);
  }
};

exports._ = _;
var RE1 = /^_?[a-z][0-9a-z_]{0,63}$/;
exports.RE1 = RE1;
var RE2 = /(\w+)\.(prod|staging|dev)\.(\w+)/;

exports.RE2 = RE2;

function checkConstructor(input) {
  for (var _len = arguments.length, constructors = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
    constructors[_key - 1] = arguments[_key];
  }

  return constructors.reduce(function (result, constructor) {
    return result || input.constructor === constructor;
  }, false);
}

// borrowed from superagent
function isObject(obj) {
  return obj === Object(obj);
}

function mSearchSerialize(obj) {
  if (!isObject(obj)) {
    return obj;
  }
  var pairs = [];
  for (var key in obj) {
    if (obj[key] != null) {
      pairs.push(encodeURIComponent(key) + '=' + encodeURIComponent(obj[key]));
    }
  }
  return pairs.join('//');
}

function set(obj, k, v) {
  var isundef = _.isUndefined(v);
  var isnull = _.isNull(v);
  var isnan = _.isNaN(v);
  if (!isundef && !isnull && !isnan) {
    obj[k] = v;
  }
  return obj[k];
}
},{}],7:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { 'default': obj }; }

var _filter = require('./filter');

var _facet = require('./facet');

var _sort = require('./sort');

var _request = require('./request');

var _engine = require('./engine');

var _engine2 = _interopRequireDefault(_engine);

var Blackbird = {
  cnfFilter: _filter.cnfFilter,
  dnfFilter: _filter.dnfFilter,
  enumFacet: _facet.enumFacet,
  histFacet: _facet.histFacet,
  rangeFacet: _facet.rangeFacet,
  ascSort: _sort.ascSort,
  descSort: _sort.descSort,
  searchRequest: _request.searchRequest,
  multiSearchRequest: _request.multiSearchRequest,
  engine: _engine2['default']
};

var globalScope = new Function('return this')();
globalScope.Blackbird = Blackbird;

exports['default'] = Blackbird;
module.exports = exports['default'];
},{"./engine":2,"./facet":4,"./filter":5,"./request":8,"./sort":9}],8:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; desc = parent = getter = undefined; _again = false; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

exports.similarRequest = similarRequest;
exports.typeaheadRequest = typeaheadRequest;
exports.searchRequest = searchRequest;
exports.multiSearchRequest = multiSearchRequest;

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _helpers = require('./helpers');

var Request = (function () {
  function Request(options) {
    _classCallCheck(this, Request);

    // todo: clean this up
    (0, _helpers.set)(this, 'start', Number(options.start));
    (0, _helpers.set)(this, 'num', Number(options.num));
    (0, _helpers.set)(this, 'sort', Request.handleSorts(options.sort));
  }

  _createClass(Request, null, [{
    key: 'handleSorts',
    value: function handleSorts(input) {
      if (Array.isArray(input)) {
        return input.map(function (sort) {
          return sort.toString();
        }).join(',');
      }
      return input;
    }
  }]);

  return Request;
})();

exports.Request = Request;

var SearchRequest = (function (_Request) {
  function SearchRequest(options) {
    _classCallCheck(this, SearchRequest);

    _get(Object.getPrototypeOf(SearchRequest.prototype), 'constructor', this).call(this, options);
    this.q = options.q || '';
    (0, _helpers.set)(this, 'fields', SearchRequest.handleFields(options.fields));
    (0, _helpers.set)(this, 'facet', SearchRequest.handleFacetsAndFilters(options.facet));
    (0, _helpers.set)(this, 'filter', SearchRequest.handleFacetsAndFilters(options.filter));
    if (!(0, _helpers.checkConstructor)(this.q, String)) {
      throw new Error('Request#q must be a string.');
    }
  }

  _inherits(SearchRequest, _Request);

  _createClass(SearchRequest, null, [{
    key: 'handleFields',
    value: function handleFields(input) {
      if (Array.isArray(input)) {
        return input.join(',');
      }
      return input;
    }
  }, {
    key: 'handleFacetsAndFilters',
    value: function handleFacetsAndFilters(input) {
      if (Array.isArray(input)) {
        return input.map(SearchRequest.handleFacetsAndFilters);
      }
      if (input) {
        return input.toString();
      }
    }
  }]);

  return SearchRequest;
})(Request);

exports.SearchRequest = SearchRequest;

var QueryComponent = function QueryComponent(options) {
  _classCallCheck(this, QueryComponent);

  this.q = options.q || '';
  (0, _helpers.set)(this, 'filter', SearchRequest.handleFacetsAndFilters(options.filter));
  if (!(0, _helpers.checkConstructor)(this.q, String)) {
    throw new Error('QueryComponent#q must be a string.');
  }
};

var MultiSearchRequest = (function (_Request2) {
  function MultiSearchRequest(options) {
    _classCallCheck(this, MultiSearchRequest);

    _get(Object.getPrototypeOf(MultiSearchRequest.prototype), 'constructor', this).call(this, options);
    var qc = options.qc;
    if (qc.length >= 6) {
      throw new Error('A multi-search only supports up to 6 queries.');
    }
    (0, _helpers.set)(this, 'qc', MultiSearchRequest.handleQc(qc));
  }

  _inherits(MultiSearchRequest, _Request2);

  _createClass(MultiSearchRequest, null, [{
    key: 'handleQc',
    value: function handleQc(qcs) {
      return qcs.map(function (qc) {
        return (0, _helpers.mSearchSerialize)(new QueryComponent(qc));
      });
    }
  }]);

  return MultiSearchRequest;
})(Request);

exports.MultiSearchRequest = MultiSearchRequest;

var TypeaheadRequest = function TypeaheadRequest(options) {
  _classCallCheck(this, TypeaheadRequest);

  this.q = options.q || '';
  if (!(0, _helpers.checkConstructor)(this.q, String)) {
    throw new Error('TypeaheadRequest#q must be a string.');
  }
};

exports.TypeaheadRequest = TypeaheadRequest;

var SimilarRequest = function SimilarRequest(options) {
  _classCallCheck(this, SimilarRequest);

  this.id = options.id;
  (0, _helpers.set)(this, 'num', options.num);
  (0, _helpers.set)(this, 'filter', SearchRequest.handleFacetsAndFilters(options.filter));
  (0, _helpers.set)(this, 'fields', SearchRequest.handleFields(options.fields));
};

exports.SimilarRequest = SimilarRequest;

function similarRequest(options) {
  return new SimilarRequest(options);
}

function typeaheadRequest(options) {
  return new TypeaheadRequest(options);
}

function searchRequest(options) {
  return new SearchRequest(options);
}

function multiSearchRequest(options) {
  return new MultiSearchRequest(options);
}
},{"./helpers":6}],9:[function(require,module,exports){
'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; desc = parent = getter = undefined; _again = false; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

exports.ascSort = ascSort;
exports.descSort = descSort;

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _helpers = require('./helpers');

var Sort = (function () {
  function Sort(options) {
    _classCallCheck(this, Sort);

    var field = options.field;

    if (!field) {
      throw new Error('Sort#field is required.');
    }
    if (!(0, _helpers.checkConstructor)(field, String)) {
      throw new Error('Sort#field must be a string.');
    }
    if (!_helpers.RE1.test(field)) {
      throw new Error('Sort#field can only contain a-z, 0-9, and underscores, and must start with a lowercase letter.');
    }
    this.field = field;
  }

  _createClass(Sort, [{
    key: 'toString',
    value: function toString(ordering) {
      return '' + this.field + ':' + ordering;
    }
  }]);

  return Sort;
})();

var AscSort = (function (_Sort) {
  function AscSort() {
    _classCallCheck(this, AscSort);

    if (_Sort != null) {
      _Sort.apply(this, arguments);
    }
  }

  _inherits(AscSort, _Sort);

  _createClass(AscSort, [{
    key: 'toString',
    value: function toString() {
      return _get(Object.getPrototypeOf(AscSort.prototype), 'toString', this).call(this, 'asc');
    }
  }]);

  return AscSort;
})(Sort);

var DescSort = (function (_Sort2) {
  function DescSort() {
    _classCallCheck(this, DescSort);

    if (_Sort2 != null) {
      _Sort2.apply(this, arguments);
    }
  }

  _inherits(DescSort, _Sort2);

  _createClass(DescSort, [{
    key: 'toString',
    value: function toString() {
      return _get(Object.getPrototypeOf(DescSort.prototype), 'toString', this).call(this, 'desc');
    }
  }]);

  return DescSort;
})(Sort);

function ascSort(options) {
  return new AscSort(options);
}

function descSort(options) {
  return new DescSort(options);
}
},{"./helpers":6}],10:[function(require,module,exports){
/**
 * Module dependencies.
 */

var Emitter = require('emitter');
var reduce = require('reduce');

/**
 * Root reference for iframes.
 */

var root = 'undefined' == typeof window
  ? (this || self)
  : window;

/**
 * Noop.
 */

function noop(){};

/**
 * Check if `obj` is a host object,
 * we don't want to serialize these :)
 *
 * TODO: future proof, move to compoent land
 *
 * @param {Object} obj
 * @return {Boolean}
 * @api private
 */

function isHost(obj) {
  var str = {}.toString.call(obj);

  switch (str) {
    case '[object File]':
    case '[object Blob]':
    case '[object FormData]':
      return true;
    default:
      return false;
  }
}

/**
 * Determine XHR.
 */

request.getXHR = function () {
  if (root.XMLHttpRequest
      && (!root.location || 'file:' != root.location.protocol
          || !root.ActiveXObject)) {
    return new XMLHttpRequest;
  } else {
    try { return new ActiveXObject('Microsoft.XMLHTTP'); } catch(e) {}
    try { return new ActiveXObject('Msxml2.XMLHTTP.6.0'); } catch(e) {}
    try { return new ActiveXObject('Msxml2.XMLHTTP.3.0'); } catch(e) {}
    try { return new ActiveXObject('Msxml2.XMLHTTP'); } catch(e) {}
  }
  return false;
};

/**
 * Removes leading and trailing whitespace, added to support IE.
 *
 * @param {String} s
 * @return {String}
 * @api private
 */

var trim = ''.trim
  ? function(s) { return s.trim(); }
  : function(s) { return s.replace(/(^\s*|\s*$)/g, ''); };

/**
 * Check if `obj` is an object.
 *
 * @param {Object} obj
 * @return {Boolean}
 * @api private
 */

function isObject(obj) {
  return obj === Object(obj);
}

/**
 * Serialize the given `obj`.
 *
 * @param {Object} obj
 * @return {String}
 * @api private
 */

function serialize(obj) {
  if (!isObject(obj)) return obj;
  var pairs = [];
  for (var key in obj) {
    if (null != obj[key]) {
      pushEncodedKeyValuePair(pairs, key, obj[key]);
    }
  }
  return pairs.join('&');
}

/**
 * Helps 'serialize' with serializing arrays.
 * Mutates the pairs array.
 *
 * @param {Array} pairs
 * @param {String} key
 * @param {Mixed} val
 */

function pushEncodedKeyValuePair(pairs, key, val) {
  if (Array.isArray(val)) {
    return val.forEach(function(v) {
      pushEncodedKeyValuePair(pairs, key, v);
    });
  }
  pairs.push(encodeURIComponent(key)
    + '=' + encodeURIComponent(val));
}

/**
 * Expose serialization method.
 */

 request.serializeObject = serialize;

 /**
  * Parse the given x-www-form-urlencoded `str`.
  *
  * @param {String} str
  * @return {Object}
  * @api private
  */

function parseString(str) {
  var obj = {};
  var pairs = str.split('&');
  var parts;
  var pair;

  for (var i = 0, len = pairs.length; i < len; ++i) {
    pair = pairs[i];
    parts = pair.split('=');
    obj[decodeURIComponent(parts[0])] = decodeURIComponent(parts[1]);
  }

  return obj;
}

/**
 * Expose parser.
 */

request.parseString = parseString;

/**
 * Default MIME type map.
 *
 *     superagent.types.xml = 'application/xml';
 *
 */

request.types = {
  html: 'text/html',
  json: 'application/json',
  xml: 'application/xml',
  urlencoded: 'application/x-www-form-urlencoded',
  'form': 'application/x-www-form-urlencoded',
  'form-data': 'application/x-www-form-urlencoded'
};

/**
 * Default serialization map.
 *
 *     superagent.serialize['application/xml'] = function(obj){
 *       return 'generated xml here';
 *     };
 *
 */

 request.serialize = {
   'application/x-www-form-urlencoded': serialize,
   'application/json': JSON.stringify
 };

 /**
  * Default parsers.
  *
  *     superagent.parse['application/xml'] = function(str){
  *       return { object parsed from str };
  *     };
  *
  */

request.parse = {
  'application/x-www-form-urlencoded': parseString,
  'application/json': JSON.parse
};

/**
 * Parse the given header `str` into
 * an object containing the mapped fields.
 *
 * @param {String} str
 * @return {Object}
 * @api private
 */

function parseHeader(str) {
  var lines = str.split(/\r?\n/);
  var fields = {};
  var index;
  var line;
  var field;
  var val;

  lines.pop(); // trailing CRLF

  for (var i = 0, len = lines.length; i < len; ++i) {
    line = lines[i];
    index = line.indexOf(':');
    field = line.slice(0, index).toLowerCase();
    val = trim(line.slice(index + 1));
    fields[field] = val;
  }

  return fields;
}

/**
 * Return the mime type for the given `str`.
 *
 * @param {String} str
 * @return {String}
 * @api private
 */

function type(str){
  return str.split(/ *; */).shift();
};

/**
 * Return header field parameters.
 *
 * @param {String} str
 * @return {Object}
 * @api private
 */

function params(str){
  return reduce(str.split(/ *; */), function(obj, str){
    var parts = str.split(/ *= */)
      , key = parts.shift()
      , val = parts.shift();

    if (key && val) obj[key] = val;
    return obj;
  }, {});
};

/**
 * Initialize a new `Response` with the given `xhr`.
 *
 *  - set flags (.ok, .error, etc)
 *  - parse header
 *
 * Examples:
 *
 *  Aliasing `superagent` as `request` is nice:
 *
 *      request = superagent;
 *
 *  We can use the promise-like API, or pass callbacks:
 *
 *      request.get('/').end(function(res){});
 *      request.get('/', function(res){});
 *
 *  Sending data can be chained:
 *
 *      request
 *        .post('/user')
 *        .send({ name: 'tj' })
 *        .end(function(res){});
 *
 *  Or passed to `.send()`:
 *
 *      request
 *        .post('/user')
 *        .send({ name: 'tj' }, function(res){});
 *
 *  Or passed to `.post()`:
 *
 *      request
 *        .post('/user', { name: 'tj' })
 *        .end(function(res){});
 *
 * Or further reduced to a single call for simple cases:
 *
 *      request
 *        .post('/user', { name: 'tj' }, function(res){});
 *
 * @param {XMLHTTPRequest} xhr
 * @param {Object} options
 * @api private
 */

function Response(req, options) {
  options = options || {};
  this.req = req;
  this.xhr = this.req.xhr;
  // responseText is accessible only if responseType is '' or 'text' and on older browsers
  this.text = ((this.req.method !='HEAD' && (this.xhr.responseType === '' || this.xhr.responseType === 'text')) || typeof this.xhr.responseType === 'undefined')
     ? this.xhr.responseText
     : null;
  this.statusText = this.req.xhr.statusText;
  this.setStatusProperties(this.xhr.status);
  this.header = this.headers = parseHeader(this.xhr.getAllResponseHeaders());
  // getAllResponseHeaders sometimes falsely returns "" for CORS requests, but
  // getResponseHeader still works. so we get content-type even if getting
  // other headers fails.
  this.header['content-type'] = this.xhr.getResponseHeader('content-type');
  this.setHeaderProperties(this.header);
  this.body = this.req.method != 'HEAD'
    ? this.parseBody(this.text ? this.text : this.xhr.response)
    : null;
}

/**
 * Get case-insensitive `field` value.
 *
 * @param {String} field
 * @return {String}
 * @api public
 */

Response.prototype.get = function(field){
  return this.header[field.toLowerCase()];
};

/**
 * Set header related properties:
 *
 *   - `.type` the content type without params
 *
 * A response of "Content-Type: text/plain; charset=utf-8"
 * will provide you with a `.type` of "text/plain".
 *
 * @param {Object} header
 * @api private
 */

Response.prototype.setHeaderProperties = function(header){
  // content-type
  var ct = this.header['content-type'] || '';
  this.type = type(ct);

  // params
  var obj = params(ct);
  for (var key in obj) this[key] = obj[key];
};

/**
 * Parse the given body `str`.
 *
 * Used for auto-parsing of bodies. Parsers
 * are defined on the `superagent.parse` object.
 *
 * @param {String} str
 * @return {Mixed}
 * @api private
 */

Response.prototype.parseBody = function(str){
  var parse = request.parse[this.type];
  return parse && str && (str.length || str instanceof Object)
    ? parse(str)
    : null;
};

/**
 * Set flags such as `.ok` based on `status`.
 *
 * For example a 2xx response will give you a `.ok` of __true__
 * whereas 5xx will be __false__ and `.error` will be __true__. The
 * `.clientError` and `.serverError` are also available to be more
 * specific, and `.statusType` is the class of error ranging from 1..5
 * sometimes useful for mapping respond colors etc.
 *
 * "sugar" properties are also defined for common cases. Currently providing:
 *
 *   - .noContent
 *   - .badRequest
 *   - .unauthorized
 *   - .notAcceptable
 *   - .notFound
 *
 * @param {Number} status
 * @api private
 */

Response.prototype.setStatusProperties = function(status){
  // handle IE9 bug: http://stackoverflow.com/questions/10046972/msie-returns-status-code-of-1223-for-ajax-request
  if (status === 1223) {
    status = 204;
  }

  var type = status / 100 | 0;

  // status / class
  this.status = status;
  this.statusType = type;

  // basics
  this.info = 1 == type;
  this.ok = 2 == type;
  this.clientError = 4 == type;
  this.serverError = 5 == type;
  this.error = (4 == type || 5 == type)
    ? this.toError()
    : false;

  // sugar
  this.accepted = 202 == status;
  this.noContent = 204 == status;
  this.badRequest = 400 == status;
  this.unauthorized = 401 == status;
  this.notAcceptable = 406 == status;
  this.notFound = 404 == status;
  this.forbidden = 403 == status;
};

/**
 * Return an `Error` representative of this response.
 *
 * @return {Error}
 * @api public
 */

Response.prototype.toError = function(){
  var req = this.req;
  var method = req.method;
  var url = req.url;

  var msg = 'cannot ' + method + ' ' + url + ' (' + this.status + ')';
  var err = new Error(msg);
  err.status = this.status;
  err.method = method;
  err.url = url;

  return err;
};

/**
 * Expose `Response`.
 */

request.Response = Response;

/**
 * Initialize a new `Request` with the given `method` and `url`.
 *
 * @param {String} method
 * @param {String} url
 * @api public
 */

function Request(method, url) {
  var self = this;
  Emitter.call(this);
  this._query = this._query || [];
  this.method = method;
  this.url = url;
  this.header = {};
  this._header = {};
  this.on('end', function(){
    var err = null;
    var res = null;

    try {
      res = new Response(self);
    } catch(e) {
      err = new Error('Parser is unable to parse the response');
      err.parse = true;
      err.original = e;
      return self.callback(err);
    }

    self.emit('response', res);

    if (err) {
      return self.callback(err, res);
    }

    if (res.status >= 200 && res.status < 300) {
      return self.callback(err, res);
    }

    var new_err = new Error(res.statusText || 'Unsuccessful HTTP response');
    new_err.original = err;
    new_err.response = res;
    new_err.status = res.status;

    self.callback(err || new_err, res);
  });
}

/**
 * Mixin `Emitter`.
 */

Emitter(Request.prototype);

/**
 * Allow for extension
 */

Request.prototype.use = function(fn) {
  fn(this);
  return this;
}

/**
 * Set timeout to `ms`.
 *
 * @param {Number} ms
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.timeout = function(ms){
  this._timeout = ms;
  return this;
};

/**
 * Clear previous timeout.
 *
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.clearTimeout = function(){
  this._timeout = 0;
  clearTimeout(this._timer);
  return this;
};

/**
 * Abort the request, and clear potential timeout.
 *
 * @return {Request}
 * @api public
 */

Request.prototype.abort = function(){
  if (this.aborted) return;
  this.aborted = true;
  this.xhr.abort();
  this.clearTimeout();
  this.emit('abort');
  return this;
};

/**
 * Set header `field` to `val`, or multiple fields with one object.
 *
 * Examples:
 *
 *      req.get('/')
 *        .set('Accept', 'application/json')
 *        .set('X-API-Key', 'foobar')
 *        .end(callback);
 *
 *      req.get('/')
 *        .set({ Accept: 'application/json', 'X-API-Key': 'foobar' })
 *        .end(callback);
 *
 * @param {String|Object} field
 * @param {String} val
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.set = function(field, val){
  if (isObject(field)) {
    for (var key in field) {
      this.set(key, field[key]);
    }
    return this;
  }
  this._header[field.toLowerCase()] = val;
  this.header[field] = val;
  return this;
};

/**
 * Remove header `field`.
 *
 * Example:
 *
 *      req.get('/')
 *        .unset('User-Agent')
 *        .end(callback);
 *
 * @param {String} field
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.unset = function(field){
  delete this._header[field.toLowerCase()];
  delete this.header[field];
  return this;
};

/**
 * Get case-insensitive header `field` value.
 *
 * @param {String} field
 * @return {String}
 * @api private
 */

Request.prototype.getHeader = function(field){
  return this._header[field.toLowerCase()];
};

/**
 * Set Content-Type to `type`, mapping values from `request.types`.
 *
 * Examples:
 *
 *      superagent.types.xml = 'application/xml';
 *
 *      request.post('/')
 *        .type('xml')
 *        .send(xmlstring)
 *        .end(callback);
 *
 *      request.post('/')
 *        .type('application/xml')
 *        .send(xmlstring)
 *        .end(callback);
 *
 * @param {String} type
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.type = function(type){
  this.set('Content-Type', request.types[type] || type);
  return this;
};

/**
 * Set Accept to `type`, mapping values from `request.types`.
 *
 * Examples:
 *
 *      superagent.types.json = 'application/json';
 *
 *      request.get('/agent')
 *        .accept('json')
 *        .end(callback);
 *
 *      request.get('/agent')
 *        .accept('application/json')
 *        .end(callback);
 *
 * @param {String} accept
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.accept = function(type){
  this.set('Accept', request.types[type] || type);
  return this;
};

/**
 * Set Authorization field value with `user` and `pass`.
 *
 * @param {String} user
 * @param {String} pass
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.auth = function(user, pass){
  var str = btoa(user + ':' + pass);
  this.set('Authorization', 'Basic ' + str);
  return this;
};

/**
* Add query-string `val`.
*
* Examples:
*
*   request.get('/shoes')
*     .query('size=10')
*     .query({ color: 'blue' })
*
* @param {Object|String} val
* @return {Request} for chaining
* @api public
*/

Request.prototype.query = function(val){
  if ('string' != typeof val) val = serialize(val);
  if (val) this._query.push(val);
  return this;
};

/**
 * Write the field `name` and `val` for "multipart/form-data"
 * request bodies.
 *
 * ``` js
 * request.post('/upload')
 *   .field('foo', 'bar')
 *   .end(callback);
 * ```
 *
 * @param {String} name
 * @param {String|Blob|File} val
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.field = function(name, val){
  if (!this._formData) this._formData = new root.FormData();
  this._formData.append(name, val);
  return this;
};

/**
 * Queue the given `file` as an attachment to the specified `field`,
 * with optional `filename`.
 *
 * ``` js
 * request.post('/upload')
 *   .attach(new Blob(['<a id="a"><b id="b">hey!</b></a>'], { type: "text/html"}))
 *   .end(callback);
 * ```
 *
 * @param {String} field
 * @param {Blob|File} file
 * @param {String} filename
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.attach = function(field, file, filename){
  if (!this._formData) this._formData = new root.FormData();
  this._formData.append(field, file, filename);
  return this;
};

/**
 * Send `data`, defaulting the `.type()` to "json" when
 * an object is given.
 *
 * Examples:
 *
 *       // querystring
 *       request.get('/search')
 *         .end(callback)
 *
 *       // multiple data "writes"
 *       request.get('/search')
 *         .send({ search: 'query' })
 *         .send({ range: '1..5' })
 *         .send({ order: 'desc' })
 *         .end(callback)
 *
 *       // manual json
 *       request.post('/user')
 *         .type('json')
 *         .send('{"name":"tj"})
 *         .end(callback)
 *
 *       // auto json
 *       request.post('/user')
 *         .send({ name: 'tj' })
 *         .end(callback)
 *
 *       // manual x-www-form-urlencoded
 *       request.post('/user')
 *         .type('form')
 *         .send('name=tj')
 *         .end(callback)
 *
 *       // auto x-www-form-urlencoded
 *       request.post('/user')
 *         .type('form')
 *         .send({ name: 'tj' })
 *         .end(callback)
 *
 *       // defaults to x-www-form-urlencoded
  *      request.post('/user')
  *        .send('name=tobi')
  *        .send('species=ferret')
  *        .end(callback)
 *
 * @param {String|Object} data
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.send = function(data){
  var obj = isObject(data);
  var type = this.getHeader('Content-Type');

  // merge
  if (obj && isObject(this._data)) {
    for (var key in data) {
      this._data[key] = data[key];
    }
  } else if ('string' == typeof data) {
    if (!type) this.type('form');
    type = this.getHeader('Content-Type');
    if ('application/x-www-form-urlencoded' == type) {
      this._data = this._data
        ? this._data + '&' + data
        : data;
    } else {
      this._data = (this._data || '') + data;
    }
  } else {
    this._data = data;
  }

  if (!obj || isHost(data)) return this;
  if (!type) this.type('json');
  return this;
};

/**
 * Invoke the callback with `err` and `res`
 * and handle arity check.
 *
 * @param {Error} err
 * @param {Response} res
 * @api private
 */

Request.prototype.callback = function(err, res){
  var fn = this._callback;
  this.clearTimeout();
  fn(err, res);
};

/**
 * Invoke callback with x-domain error.
 *
 * @api private
 */

Request.prototype.crossDomainError = function(){
  var err = new Error('Origin is not allowed by Access-Control-Allow-Origin');
  err.crossDomain = true;
  this.callback(err);
};

/**
 * Invoke callback with timeout error.
 *
 * @api private
 */

Request.prototype.timeoutError = function(){
  var timeout = this._timeout;
  var err = new Error('timeout of ' + timeout + 'ms exceeded');
  err.timeout = timeout;
  this.callback(err);
};

/**
 * Enable transmission of cookies with x-domain requests.
 *
 * Note that for this to work the origin must not be
 * using "Access-Control-Allow-Origin" with a wildcard,
 * and also must set "Access-Control-Allow-Credentials"
 * to "true".
 *
 * @api public
 */

Request.prototype.withCredentials = function(){
  this._withCredentials = true;
  return this;
};

/**
 * Initiate request, invoking callback `fn(res)`
 * with an instanceof `Response`.
 *
 * @param {Function} fn
 * @return {Request} for chaining
 * @api public
 */

Request.prototype.end = function(fn){
  var self = this;
  var xhr = this.xhr = request.getXHR();
  var query = this._query.join('&');
  var timeout = this._timeout;
  var data = this._formData || this._data;

  // store callback
  this._callback = fn || noop;

  // state change
  xhr.onreadystatechange = function(){
    if (4 != xhr.readyState) return;

    // In IE9, reads to any property (e.g. status) off of an aborted XHR will
    // result in the error "Could not complete the operation due to error c00c023f"
    var status;
    try { status = xhr.status } catch(e) { status = 0; }

    if (0 == status) {
      if (self.timedout) return self.timeoutError();
      if (self.aborted) return;
      return self.crossDomainError();
    }
    self.emit('end');
  };

  // progress
  var handleProgress = function(e){
    if (e.total > 0) {
      e.percent = e.loaded / e.total * 100;
    }
    self.emit('progress', e);
  };
  if (this.hasListeners('progress')) {
    xhr.onprogress = handleProgress;
  }
  try {
    if (xhr.upload && this.hasListeners('progress')) {
      xhr.upload.onprogress = handleProgress;
    }
  } catch(e) {
    // Accessing xhr.upload fails in IE from a web worker, so just pretend it doesn't exist.
    // Reported here:
    // https://connect.microsoft.com/IE/feedback/details/837245/xmlhttprequest-upload-throws-invalid-argument-when-used-from-web-worker-context
  }

  // timeout
  if (timeout && !this._timer) {
    this._timer = setTimeout(function(){
      self.timedout = true;
      self.abort();
    }, timeout);
  }

  // querystring
  if (query) {
    query = request.serializeObject(query);
    this.url += ~this.url.indexOf('?')
      ? '&' + query
      : '?' + query;
  }

  // initiate request
  xhr.open(this.method, this.url, true);

  // CORS
  if (this._withCredentials) xhr.withCredentials = true;

  // body
  if ('GET' != this.method && 'HEAD' != this.method && 'string' != typeof data && !isHost(data)) {
    // serialize stuff
    var serialize = request.serialize[this.getHeader('Content-Type')];
    if (serialize) data = serialize(data);
  }

  // set header fields
  for (var field in this.header) {
    if (null == this.header[field]) continue;
    xhr.setRequestHeader(field, this.header[field]);
  }

  // send stuff
  this.emit('request', this);
  xhr.send(data);
  return this;
};

/**
 * Expose `Request`.
 */

request.Request = Request;

/**
 * Issue a request:
 *
 * Examples:
 *
 *    request('GET', '/users').end(callback)
 *    request('/users').end(callback)
 *    request('/users', callback)
 *
 * @param {String} method
 * @param {String|Function} url or callback
 * @return {Request}
 * @api public
 */

function request(method, url) {
  // callback
  if ('function' == typeof url) {
    return new Request('GET', method).end(url);
  }

  // url first
  if (1 == arguments.length) {
    return new Request('GET', method);
  }

  return new Request(method, url);
}

/**
 * GET `url` with optional callback `fn(res)`.
 *
 * @param {String} url
 * @param {Mixed|Function} data or fn
 * @param {Function} fn
 * @return {Request}
 * @api public
 */

request.get = function(url, data, fn){
  var req = request('GET', url);
  if ('function' == typeof data) fn = data, data = null;
  if (data) req.query(data);
  if (fn) req.end(fn);
  return req;
};

/**
 * HEAD `url` with optional callback `fn(res)`.
 *
 * @param {String} url
 * @param {Mixed|Function} data or fn
 * @param {Function} fn
 * @return {Request}
 * @api public
 */

request.head = function(url, data, fn){
  var req = request('HEAD', url);
  if ('function' == typeof data) fn = data, data = null;
  if (data) req.send(data);
  if (fn) req.end(fn);
  return req;
};

/**
 * DELETE `url` with optional callback `fn(res)`.
 *
 * @param {String} url
 * @param {Function} fn
 * @return {Request}
 * @api public
 */

request.del = function(url, fn){
  var req = request('DELETE', url);
  if (fn) req.end(fn);
  return req;
};

/**
 * PATCH `url` with optional `data` and callback `fn(res)`.
 *
 * @param {String} url
 * @param {Mixed} data
 * @param {Function} fn
 * @return {Request}
 * @api public
 */

request.patch = function(url, data, fn){
  var req = request('PATCH', url);
  if ('function' == typeof data) fn = data, data = null;
  if (data) req.send(data);
  if (fn) req.end(fn);
  return req;
};

/**
 * POST `url` with optional `data` and callback `fn(res)`.
 *
 * @param {String} url
 * @param {Mixed} data
 * @param {Function} fn
 * @return {Request}
 * @api public
 */

request.post = function(url, data, fn){
  var req = request('POST', url);
  if ('function' == typeof data) fn = data, data = null;
  if (data) req.send(data);
  if (fn) req.end(fn);
  return req;
};

/**
 * PUT `url` with optional `data` and callback `fn(res)`.
 *
 * @param {String} url
 * @param {Mixed|Function} data or fn
 * @param {Function} fn
 * @return {Request}
 * @api public
 */

request.put = function(url, data, fn){
  var req = request('PUT', url);
  if ('function' == typeof data) fn = data, data = null;
  if (data) req.send(data);
  if (fn) req.end(fn);
  return req;
};

/**
 * Expose `request`.
 */

module.exports = request;

},{"emitter":11,"reduce":12}],11:[function(require,module,exports){

/**
 * Expose `Emitter`.
 */

module.exports = Emitter;

/**
 * Initialize a new `Emitter`.
 *
 * @api public
 */

function Emitter(obj) {
  if (obj) return mixin(obj);
};

/**
 * Mixin the emitter properties.
 *
 * @param {Object} obj
 * @return {Object}
 * @api private
 */

function mixin(obj) {
  for (var key in Emitter.prototype) {
    obj[key] = Emitter.prototype[key];
  }
  return obj;
}

/**
 * Listen on the given `event` with `fn`.
 *
 * @param {String} event
 * @param {Function} fn
 * @return {Emitter}
 * @api public
 */

Emitter.prototype.on =
Emitter.prototype.addEventListener = function(event, fn){
  this._callbacks = this._callbacks || {};
  (this._callbacks[event] = this._callbacks[event] || [])
    .push(fn);
  return this;
};

/**
 * Adds an `event` listener that will be invoked a single
 * time then automatically removed.
 *
 * @param {String} event
 * @param {Function} fn
 * @return {Emitter}
 * @api public
 */

Emitter.prototype.once = function(event, fn){
  var self = this;
  this._callbacks = this._callbacks || {};

  function on() {
    self.off(event, on);
    fn.apply(this, arguments);
  }

  on.fn = fn;
  this.on(event, on);
  return this;
};

/**
 * Remove the given callback for `event` or all
 * registered callbacks.
 *
 * @param {String} event
 * @param {Function} fn
 * @return {Emitter}
 * @api public
 */

Emitter.prototype.off =
Emitter.prototype.removeListener =
Emitter.prototype.removeAllListeners =
Emitter.prototype.removeEventListener = function(event, fn){
  this._callbacks = this._callbacks || {};

  // all
  if (0 == arguments.length) {
    this._callbacks = {};
    return this;
  }

  // specific event
  var callbacks = this._callbacks[event];
  if (!callbacks) return this;

  // remove all handlers
  if (1 == arguments.length) {
    delete this._callbacks[event];
    return this;
  }

  // remove specific handler
  var cb;
  for (var i = 0; i < callbacks.length; i++) {
    cb = callbacks[i];
    if (cb === fn || cb.fn === fn) {
      callbacks.splice(i, 1);
      break;
    }
  }
  return this;
};

/**
 * Emit `event` with the given args.
 *
 * @param {String} event
 * @param {Mixed} ...
 * @return {Emitter}
 */

Emitter.prototype.emit = function(event){
  this._callbacks = this._callbacks || {};
  var args = [].slice.call(arguments, 1)
    , callbacks = this._callbacks[event];

  if (callbacks) {
    callbacks = callbacks.slice(0);
    for (var i = 0, len = callbacks.length; i < len; ++i) {
      callbacks[i].apply(this, args);
    }
  }

  return this;
};

/**
 * Return array of callbacks for `event`.
 *
 * @param {String} event
 * @return {Array}
 * @api public
 */

Emitter.prototype.listeners = function(event){
  this._callbacks = this._callbacks || {};
  return this._callbacks[event] || [];
};

/**
 * Check if this emitter has `event` handlers.
 *
 * @param {String} event
 * @return {Boolean}
 * @api public
 */

Emitter.prototype.hasListeners = function(event){
  return !! this.listeners(event).length;
};

},{}],12:[function(require,module,exports){

/**
 * Reduce `arr` with `fn`.
 *
 * @param {Array} arr
 * @param {Function} fn
 * @param {Mixed} initial
 *
 * TODO: combatible error handling?
 */

module.exports = function(arr, fn, initial){  
  var idx = 0;
  var len = arr.length;
  var curr = arguments.length == 3
    ? initial
    : arr[idx++];

  while (idx < len) {
    curr = fn.call(null, curr, arr[idx], ++idx, arr);
  }
  
  return curr;
};
},{}]},{},[1]);