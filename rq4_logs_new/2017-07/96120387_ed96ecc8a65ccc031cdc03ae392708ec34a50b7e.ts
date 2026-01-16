import api = require('..');
import assert = require('assert');
import http = require('http');
import url = require('url');

describe('archol-dev-server-tests', function () {

    before(function (done) {
        api.loadConfig(__dirname + '/sample');
        api.startServer(done);
    });

    after(function (done) {
        api.stopServer(done);
    });

    it('ping', function (done) {
        httpExpect('/~ads/ping', /20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\dZ/g, done);
    });

    it('echo', function (done) {
        httpExpect('/~sample/echo?s=123', '123', done);
    });

    it('html with injected code - index.html', function (done) {
        httpExpect('/~samplesite/index.html', '<html><body>ok<script src="~samplesite/index.js"></script></body></html>', done);
    });

    it.only('html with injected code - service fiile index.html', function (done) {
        httpExpect('/~samplefile', '<html><body>ok<script src="~samplesite/index.js"></script></body></html>', done);
    });

    it('html with injected code - nobody.html', function (done) {
        httpExpectError('/~samplesite/nobody.html', 200, done);
    });

    it('html with injected code - nofile.html', function (done) {
        httpExpectError('/~samplesite/nofile.html', 404, done);
    });

    it('html with injected code - no route', function (done) {
        httpExpectError('', 404, done);
    });

    it('html with injected code - default', function (done) {
        httpExpect('/~samplesite/', '<html><body>ok<script src="~samplesite/index.js"></script></body></html>', done);
    });

    it('try registerPlugin after startServer', function (done) {
        try {
            api.registerPlugin({ handlers: {}, injections: [] });
            assert.equal('no error', 'Server yet started error');
            done();
        } catch (err) {
            done();
        }
    });
});

describe('archol-dev-server-tests', function () {
    it('with out package.json', function () {
        if (api.loadConfig(__dirname + '/../bin'))
            assert.equal('no error', 'loadConfig must fail');
    });
    it('invalid package.json', function () {
        if (api.loadConfig(__dirname + '/invalid/1'))
            assert.equal('no error', 'loadConfig must fail');
    });
    it('invalid plugins in package.json', function () {
        if (api.loadConfig(__dirname + '/invalid/2'))
            assert.equal('no error', 'loadConfig must fail');
    });
    it('invalid file in package.json', function () {
        try {
            if (api.loadConfig(__dirname + '/invalid/3'))
                assert.equal('no error', 'loadConfig has invalid plugin');
        } catch (err) {
            //
        }        
    });
});

function httpExpect(link: string, expected: string | RegExp, callback: () => void) {
    http.get(api.serverLink(link), function (res) {
        if (res.statusCode === 301) {
            let u = url.parse(link);
            let l = res.headers.location;
            link = Array.isArray(l) ? l.join('') : l as string;
            u.pathname = link;
            httpExpect(link, expected, callback);
            return
        }
        assert.equal(200, res.statusCode);
        var data = '';

        res.on('data', function (chunk) {
            data += chunk;
        });

        res.on('error', function (err) {
            assert.equal(err.message || err.toString(), 'no error');
            callback();
        });

        res.on('end', function () {
            if (expected instanceof RegExp && !expected.test(data))
                assert.equal(data, expected.source, link);

            if (typeof expected === 'string')
                assert.equal(data, expected, link);
            callback();
        });
    });
}

function httpExpectError(link: string, expectedError: number, callback: () => void) {
    http.get(api.serverLink(link), function (res) {
        if (res.statusCode === 301) {
            let u = url.parse(link);
            let l = res.headers.location;
            link = Array.isArray(l) ? l.join('') : l as string;
            u.pathname = link;
            httpExpectError(link, expectedError, callback);
            return
        }
        assert.equal(expectedError, res.statusCode);
        callback();
    });
}