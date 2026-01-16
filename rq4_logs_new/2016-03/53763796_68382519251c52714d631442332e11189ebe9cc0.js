var request = require('request'),
  fs = require('fs'),
  SimpleTokenClient = require('simple-token-client');

function StorageApi (config) {
  this.config = config;
  this.tokenClient = new SimpleTokenClient(config);
}

function jsonResponseHandler (done) {
  return function (err, res, body) {
    if(err) {
      return done(err);
    }

    try {
      body = JSON.parse(body) || body;
    }
    catch (ex) {
      return done(new Error('response is not json'));
    }
    if(res.statusCode !== 200) {
      err = new Error(body.errmsg);
      err.code = body.errcode;
      return done(err);
    }
    done(null, body);
  };
}

function requestForm (method, url, options, formData, done) {
  var opts = {
    url: url,
    proxy: false
  };
  opts = _.merge(opts, options);
  method = method.toLowerCase();

  var req = request[method](opts, jsonResponseHandler(done));
  var form = req.form();
  _.each(formData, function (val, key) {
    form.append(key, val);
  });
}

function request (url, method, options, done) {
  var opts = {
      url: url,
      method: method,
      proxy: false,
      followRedirect: false
    };
  opts = _.merge(opts, options);

  request(opts, jsonResponseHandler(done));
}

function postFile (url, options, formData, files, done) {
  _.each(files, function (val, key) {
    formData[key] = fs.createReadStream(val);
  });
  requestForm('post', url, options, formData, done);
}

StorageApi.prototype.createFile = function (bucket, path, file, done) {
  var self = this;
  self.tokenClient.getToken(function (err, token) {
    if(err) {
      return done(new Error('failed to get access token:' + err.message));
    }
    postFile(self.config.endpoint + '/api/v1/file',
      {
        headers : {
          Authorization: 'Bearer ' + token
        }
      },
      {
        bucket: bucket,
        path: path
      },
      {
        file: file.path
      },
      done);
  });
};

module.exports = StorageApi;