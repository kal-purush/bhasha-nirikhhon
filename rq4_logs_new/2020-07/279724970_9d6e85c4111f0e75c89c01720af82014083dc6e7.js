'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.validationRules = exports.loginUser = exports.getLoginUser = undefined;

var _userModel = require('../models/userModel');

var _userModel2 = _interopRequireDefault(_userModel);

var _dotenv = require('dotenv');

var _dotenv2 = _interopRequireDefault(_dotenv);

var _jsonwebtoken = require('jsonwebtoken');

var _jsonwebtoken2 = _interopRequireDefault(_jsonwebtoken);

var _bcryptjs = require('bcryptjs');

var _bcryptjs2 = _interopRequireDefault(_bcryptjs);

var _expressValidator = require('express-validator');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

_dotenv2.default.config();
var secret = process.env.SECRET;

var getLoginUser = exports.getLoginUser = function () {
  var _ref = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee(req, res) {
    var user;
    return regeneratorRuntime.wrap(function _callee$(_context) {
      while (1) {
        switch (_context.prev = _context.next) {
          case 0:
            _context.prev = 0;
            _context.next = 3;
            return _userModel2.default.findById(req.user.id).select('-password');

          case 3:
            user = _context.sent;

            res.status(200).json(user);
            _context.next = 11;
            break;

          case 7:
            _context.prev = 7;
            _context.t0 = _context['catch'](0);

            console.error(_context.t0.message);
            res.status(500).send({ msg: 'Internal server error' });

          case 11:
          case 'end':
            return _context.stop();
        }
      }
    }, _callee, undefined, [[0, 7]]);
  }));

  return function getLoginUser(_x, _x2) {
    return _ref.apply(this, arguments);
  };
}();

var loginUser = exports.loginUser = function () {
  var _ref2 = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee2(req, res) {
    var _req$body, email, password, user, isMatch, payload;

    return regeneratorRuntime.wrap(function _callee2$(_context2) {
      while (1) {
        switch (_context2.prev = _context2.next) {
          case 0:
            _context2.prev = 0;
            _req$body = req.body, email = _req$body.email, password = _req$body.password;

            //check if email exists

            _context2.next = 4;
            return _userModel2.default.findOne({ email: email });

          case 4:
            user = _context2.sent;

            if (user) {
              _context2.next = 7;
              break;
            }

            return _context2.abrupt('return', res.status(400).json({ msg: 'No such emails' }));

          case 7:
            _context2.next = 9;
            return _bcryptjs2.default.compare(password, user.password);

          case 9:
            isMatch = _context2.sent;


            if (!isMatch) {
              res.status(400).json({ msg: 'invalid credentials' });
            }

            //create payload with user id
            payload = {
              user: {
                id: user.id
              }
            };

            //create jwt with payload

            _jsonwebtoken2.default.sign(payload, secret, {
              expiresIn: '10h'
            }, function (err, token) {
              if (err) throw err;
              res.json({ token: token });
            });
            _context2.next = 19;
            break;

          case 15:
            _context2.prev = 15;
            _context2.t0 = _context2['catch'](0);

            console.log(_context2.t0.message);
            res.status(500).send({ msg: 'Internal server error' });

          case 19:
          case 'end':
            return _context2.stop();
        }
      }
    }, _callee2, undefined, [[0, 15]]);
  }));

  return function loginUser(_x3, _x4) {
    return _ref2.apply(this, arguments);
  };
}();

var validationRules = exports.validationRules = function validationRules(method) {
  switch (method) {
    case 'userLogin':
      {
        return [
        //email required
        (0, _expressValidator.body)('email', 'Please include a valid email.').isEmail(),

        // password must be at least 6 chars long
        (0, _expressValidator.body)('password').isLength({ min: 6 })];
      }
  }
};
//# sourceMappingURL=authController.js.map