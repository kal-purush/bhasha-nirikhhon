'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var fs = require('fs');
var Promise = require('bluebird');
var sharp = require('sharp');
var path = require('path');
var mkdirp = require('mkdirp');
Promise.promisifyAll(fs);

var photosDir = '../public/photos';

var imagePresets = {
  thumb: 100,
  medium: 800,
  large: 1200,
  huge: 1600
};

exports.default = function (imageObject) {
  var versions = {};
  return fs.readFileAsync(imageObject.path).then(function (imageBuffer) {
    var _iteratorNormalCompletion = true;
    var _didIteratorError = false;
    var _iteratorError = undefined;

    try {
      for (var _iterator = Object.entries(imagePresets)[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
        var _step$value = _slicedToArray(_step.value, 2),
            version = _step$value[0],
            width = _step$value[1];

        var outfile = path.join(photosDir, version, imageObject.relativePath);
        mkdirp(path.dirname(outfile));
        runResize({
          image: imageBuffer,
          outfile: outfile,
          width: width
        });
        versions[version] = {
          path: outfile
        };
      }
    } catch (err) {
      _didIteratorError = true;
      _iteratorError = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion && _iterator.return) {
          _iterator.return();
        }
      } finally {
        if (_didIteratorError) {
          throw _iteratorError;
        }
      }
    }
  }).then(function () {
    return versions;
  });
};

var runResize = function runResize(config) {
  console.log(config);
  return sharp(config.image).resize(config.width).withMetadata().toFile(config.outfile).then(function () {
    // console.log(data)
  }).catch(function (err) {
    console.log('resizing failed ' + err);
  });
};