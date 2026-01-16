declare var require: (string) => any;
declare var describe: (string, any) => void;

var assert = require('chai').assert;
var Gobo = require("../../gobo.debug.js").Gobo;
var Test = require("./test-help.js").Tester;
var watch = require("watchjs");

describe('Filters', function () {

    Test.should('convert to upper and lower case').using(
        `<div>
            <span id='upper' g-text='name | uppercase'></span>
            <span id='lower' g-text='name | lowercase'></span>
        </div>`
    ).in((done, $) => {
        new Gobo({ watch: watch }).bind($.body, { name: "Veal" });
        assert.equal( $.cleanup($.textById('upper')), "VEAL" );
        assert.equal( $.cleanup($.textById('lower')), "veal" );
        done();
    });

});
