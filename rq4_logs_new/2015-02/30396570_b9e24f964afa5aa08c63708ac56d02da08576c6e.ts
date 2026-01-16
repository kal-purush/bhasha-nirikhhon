declare var require: (string) => any;
declare var describe: (string, any) => void;

var assert = require('chai').assert;
var Gobo = require("../../gobo.debug.js").Gobo;
var Test = require("./test-help.js").Tester;
var watch = require("watchjs");

describe('Each blocks', function () {

    Test.should('iterate over values').using(
        `<ul id='names'>
            <li g-each-name='names'>
                <span g-text="name"></span>
            </li>
        </ul>`
    ).in((done, $) => {
        var data = {
            names: [ "Veal Steakface", "Lug ThickNeck", "Big McLargeHuge" ]
        };

        new Gobo({ document: $.document, watch: watch }).bind($.body, data);
        assert.equal(
            $.cleanup($.textById('names')),
            "Veal Steakface Lug ThickNeck Big McLargeHuge"
        );

        done();
    });

});


