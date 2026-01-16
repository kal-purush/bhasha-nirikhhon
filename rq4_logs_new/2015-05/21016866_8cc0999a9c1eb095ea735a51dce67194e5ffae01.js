/**LICENSE
Copyright (c) 2015 Barry Loper
Adapted from node-pypi by Lukasz Balcerzak https://github.com/lukaszb/pypi

Copyright (c) 2011 Lukasz Balcerzak

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

Authors ordered by first contribution

- Lukasz Balcerzak <lukaszbalcerzak@gmail.com>
- Kenneth Falck <kennu@iki.fi>
*/

var PyPi, DEFAULT_URL, xmlrpc;

xmlrpc = require('xmlrpc');

DEFAULT_URL = 'http://pypi.python.org/pypi';

PyPi = function() {
  this.url = DEFAULT_URL;
  this.xmlrpcClient = xmlrpc.createClient(this.url);
}

PyPi.prototype.setUrl = function(url){
  this.url = url;
  this.xmlrpcClient = xmlrpc.createClient(this.url);
};

PyPi.prototype.callXmlrpc = function(method, args, callback, onError) {
  return this.xmlrpcClient.methodCall(method, args, function(error, value) {
    if (error && onError) {
      return onError(error);
    } else if (callback) {
      return callback(value);
    }
  });
};

PyPi.prototype.getPackageReleases = function(pkg, callback, onError, showHidden) {
  if (showHidden == null) {
    showHidden = false;
  }
  return this.callXmlrpc("package_releases", [pkg, showHidden], callback, onError);
};

PyPi.prototype.getPackagesList = function(callback, onError) {
  return this.callXmlrpc("list_packages", [], callback, onError);
};

PyPi.prototype.getPackageRoles = function(pkg, callback, onError) {
  return this.callXmlrpc("package_roles", [pkg], callback, onError);
};

PyPi.prototype.getUserPackages = function(pkg, callback, onError) {
  return this.callXmlrpc("user_packages", [pkg], callback, onError);
};

PyPi.prototype.getReleaseData = function(pkg, version, callback, onError) {
  return this.callXmlrpc("release_data", [pkg, version], callback, onError);
};

PyPi.prototype.getReleaseDownloads = function(pkg, version, callback, onError) {
  return this.callXmlrpc("release_downloads", [pkg, version], callback, onError);
};

PyPi.prototype.getReleaseUrls = function(pkg, version, callback, onError) {
  return this.callXmlrpc("release_urls", [pkg, version], callback, onError);
};

PyPi.prototype.search = function(pkg, callback, onError) {
  return this.callXmlrpc("search", [
    {
      name: pkg
    }
  ], callback, onError);
};

module.exports = new PyPi();