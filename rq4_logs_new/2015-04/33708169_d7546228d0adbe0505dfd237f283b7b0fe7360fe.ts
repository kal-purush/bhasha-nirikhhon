/// <reference path="../_definitions.d.ts" />

requirejs.config({
    //baseUrl: "../",

    paths: {
        "knockout": "../bower_components/knockoutjs/dist/knockout.debug",
        "underscore": "../bower_components/underscore/underscore",
        "kounderscore": "../src/kounderscore",

        "mocha": "../bower_components/mocha/mocha",
        "should": "../bower_components/should/should",
        "sinon": "../bower_components/sinon/sinon"
    },

    shim: {
        mocha: {
            exports: "mocha"
        }
    }
});

(<any>window).console = window.console || function () { return; };
(<any>window).notrack = true;

var tests = [
    "test.underscore",
    "test.knockout"
];

require(tests, function () {
    mocha.run();
});