/// <reference path="../external/mocha.d.ts" />
/// <reference path="../external/chai.d.ts" />
/// <reference path="../external/shelljs.d.ts" />

import sh = require('shelljs');
import chai = require('chai');
var assert = chai.assert;

function tmcat(args) {
    return sh.exec('../tmcat ' + args, {silent:true}).output;
}

describe('tmcat', function () {
    function clean() {
        sh.rm('-f', 'MyModule.ts');
    }

    beforeEach(clean);
    after(clean);

    it('should generate a .ts file correctly', function () {
        tmcat('MyModule -c ":"');
        var actual = sh.cat('MyModule.ts');
        var expected = sh.cat('correct_MyModule.ts');
        assert.equal(actual, expected);
    });

    it('should translate error messages correctly', function () {
        var actual = tmcat('MyModule -c "cat blah.tsc"');
        var expected = sh.cat('blah.tmcat');
        assert.equal(actual, expected);
    });
});