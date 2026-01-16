declare var require: (string) => any;
declare var describe: (string, any) => void;

var assert = require('assert');
var Gobo = require("../../gobo.debug.js").Gobo;
var Test = require("./test-help.js").Tester;

describe('Gobo', function () {

    Test.should('bind values to text content').using(
        `<ul>
            <li>
                <span>Name:</span>
                <span g-text="name" id='name' class='bold'></span>
            </li>
            <li>
                <span>Age:</span>
                <span g-text="age" id='age' class='subtle'></span>
            </li>
        </ul>`
    ).in((done, $) => {
        new Gobo({ document: $.document }).bind( $.body, {
            name: "Jack",
            age: 31
        });
        assert.equal( $.textById('name'), "Jack" );
        assert.equal( $.textById('age'), "31" );
        done();
    });

});
