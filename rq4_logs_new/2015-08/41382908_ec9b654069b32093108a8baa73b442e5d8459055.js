/**
 * Updated by crivas on 08/18/2015
 * Email: chester.rivas@gmail.com
 * Plugin Name: gulp-sass-importer
 */

'use strict';

var jsonfile = require('jsonfile'),
  fs = require('fs'),
  through = require('through2'),
  gutil = require('gulp-util'),
  _ = require('underscore-node');

/**
 * dynamically imports sass files based on folder names
 */
var sassImportInjector = function (options) {

  var packagesJSON;

  if (!_.isUndefined(options.packageJSON)) {
    if (typeof options.packageJSON === 'string') {
      packagesJSON = jsonfile.readFileSync(options.packageJSON);
    } else if (typeof options.packageJSON === 'object') {
      packagesJSON = options.packageJSON;
    }
  } else {
    packagesJSON = './ute-package.json';
  }

  /**
   * adds import string to the sass files
   */
  var injectImports = function (sassFile) {

    var newSass = sassFile;

    newSass += '\n';
    newSass += '// ute imports\n';

    _.each(packagesJSON.components, function (moduleName, key) {
      if (moduleName) {
        newSass += '@import "' + key + '/' + key + '";\n';
      }
    });

    newSass += '\n';
    newSass += '// brand imports\n';
    newSass += '@import "brand/' + packagesJSON.selectedBrand.toLowerCase() + '/core/core";\n';

    _.each(packagesJSON.components, function (moduleName, key) {
      if (moduleName) {
        newSass += '@import "brand/' + packagesJSON.selectedBrand.toLowerCase() + '/' + key + '/' + key + '";\n';
      }
    });

    return newSass;

  };

  /**
   * buffer each content
   * @param file
   * @param enc
   * @param callback
   */
  var bufferedContents = function (file, enc, callback) {

    if (file.isStream()) {

      this.emit('error', new gutil.PluginError('sassImportInjector', 'Streams are not supported!'));
      callback();

    } else if (file.isNull()) {

      callback(null, file); // Do nothing if no contents

    } else {

      var ctx = file.contents.toString('utf8'),
        sassAfterImports = injectImports(ctx);

      file.contents = new Buffer(sassAfterImports);
      callback(null, file);

    }

  };

  /**
   * returns streamed content
   */
  return through.obj(bufferedContents);

};

module.exports = sassImportInjector;