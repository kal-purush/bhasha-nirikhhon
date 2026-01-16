if (!process.env.PUT_IO_TOKEN) {
  console.error('Missing PUT_IO_TOKEN.');
  process.exit(0);
}

// var _ = require('lodash');
var async = require('async');
var request = require('request');
var shelljs = require('shelljs');

var COLOURS = {
  FAIL: ' \033[0;31;49m[==]\033[0m ',
  GOOD: ' \033[0;32;49m[==]\033[0m ',
  WARN: ' \033[0;33;49m[==]\033[0m ',
  TASK: ' \033[0;34;49m[==]\033[0m ',
  USER: ' \033[1;1;49m[==]\033[0m '
};
var PUT_IO_TOKEN = process.env.PUT_IO_TOKEN;

var download = function download(file, next) {
  if (Array.isArray(file)) {
    return async.each(file, download, next);
  }

  if (file.content_type.indexOf('video/') !== 0) {
    // console.error('This file is not a video: ' + JSON.stringify(file));
    return next();
  }

  if (!file.id) {
    // console.error('Unable to get file id from file: ' + JSON.stringify(file));
    return next();
  }

  if (!file.name) {
    // console.error('Unable to get filename from file: ' + JSON.stringify(file));
    return next();
  }

  console.log(file.name);

  putio({url: '/files/' + file.id + '/download'}, function (error, res) {
    if (error) {
      return next(error);
    }

    console.log(res.headers);
    next();
  });
};

var putio = function (options, done) {
  options = options || {};
  options.json = true;
  options.qs = options.qs || {};
  options.url = 'https://api.put.io/v2' + options.url;

  options.qs.oauth_token = process.env.PUT_IO_TOKEN;

  console.log(options);

  request(options, function (error, res, body) {
    res = res || {};
    done(error, {
      status: res.statusCode || 500,
      headers: res.headers || {},
      body: body || null
    });
  });
};

async.each(
  process.argv.slice(2),
  function (file_id, next) {
    console.log(file_id);

    if (isNaN(parseInt(file_id, 10))) {
      return next(COLOURS.FAIL + file_id + ' is not a valid number.');
    }

    var filename = null;
    var fileurl = null;

    putio({url: '/files/' + file_id}, function (error, res) {
      if (error) {
        return next(error);
      }
      if (res.status !== 200 || !res.body || !res.body.file) {
        return next(new Error('Failed to get file from Put.IO: ' + JSON.stringify(res.body)));
      }

      if (res.body.file.content_type === 'application/x-directory') {
        return putio({qs: {parent_id: file_id}, url: '/files/list'}, function (error, res) {
          if (error) {
            return next(error);
          }

          async.each(res.body.files, download, next);
        });
      }
      else {
        download(res.body.file, next);
      }
    });
  },
  function (error) {
    if (error) {
      console.error(error);
    }

    console.log('Finished!');
    process.exit(error ? 1 : 0);
  }
);