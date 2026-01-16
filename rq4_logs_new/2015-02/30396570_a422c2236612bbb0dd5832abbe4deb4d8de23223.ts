declare var require: (string) => any;
declare var describe: (string, any) => void;

var assert = require('chai').assert;
var Gobo = require("../../gobo.debug.js").Gobo;
var Test = require("./test-help.js").Tester;
var watch = require("watchjs");

describe('Expressions', function () {

    Test.should('allow single quotes').using(
        `<div>
            <span id='veal' g-text="veal.'full name'"></span>
            <span id='lug' g-text="lug.'full.name'"></span>
        </div>`
    ).in((done, $) => {
        new Gobo().bind($.body, {
            veal: { 'full name': "Veal Steakface" },
            lug: { 'full.name': "Lug ThickNeck" }
        });

        assert.equal( $.cleanup($.textById('veal')), "Veal Steakface" );
        assert.equal( $.cleanup($.textById('lug')), "Lug ThickNeck" );

        done();
    });

    Test.should('allow double quotes').using(
        `<div>
            <span id='veal' g-text='veal."full name"'></span>
            <span id='lug' g-text='lug."full.name"'></span>
        </div>`
    ).in((done, $) => {
        new Gobo().bind($.body, {
            veal: { 'full name': "Veal Steakface" },
            lug: { 'full.name': "Lug ThickNeck" }
        });

        assert.equal( $.cleanup($.textById('veal')), "Veal Steakface" );
        assert.equal( $.cleanup($.textById('lug')), "Lug ThickNeck" );

        done();
    });

});