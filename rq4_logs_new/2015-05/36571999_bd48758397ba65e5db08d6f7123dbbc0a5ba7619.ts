/* Globals - jQuery, $, Backbone, _ */
/// <reference path="lib/jquery/jquery.d.ts"/>
/// <reference path="lib/lodash/lodash.d.ts"/>
/// <reference path="lib/backbone/backbone.d.ts"/>


require.config({
    baseUrl: "js",
    paths: {
        "jquery": 'lib/jquery/jquery-2.1.4.min',
        "underscore": 'lib/lodash/lodash-min',
        "backbone": 'lib/backbone/backbone-min',
        "text": 'lib/require/text'
    },
    shim: {
        underscore: {
            exports: '_'
        },
        backbone: {
            deps: ['underscore', 'jquery'],
            exports: 'Backbone'
        }
    }
});

import $ = require('jquery');
import AppView = require('views/app-view');

// Load the application once the DOM is ready
$(() => {
    new AppView.AppView();
});