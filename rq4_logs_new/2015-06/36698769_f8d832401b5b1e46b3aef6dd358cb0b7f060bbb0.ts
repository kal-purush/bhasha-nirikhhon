
/// <reference path='typings/tsd.d.ts' />

import async = require('async');
import path = require('path');
import api = require('./index');

async.series<any>([
    function(done) {
        console.log('TEST: init');
        api.init(function() {
            done(null, null);
        });
    },
    function(done) {
        console.log('TEST: list');
        api.list('/storage/external_storage/sda1/test_cases', function(err, list) {
            console.log(list);
            done(err, null);
        });
    },
    function(done) {
        console.log('TEST: list features');
        api.pm_list_features(function(err, feature_list) {
            console.log(feature_list);
            done(err, null);
        });
    },
    function(done) {
        console.log('TEST: list packages');
        api.pm_list_packages(function(err, package_info_list) {
            console.log(package_info_list);
            done(null, null);
        });
    },
    function(done) {
        console.log('TEST: list instrumentation');
        api.pm_list_instrumentation(function(err, instrumentation_info_list) {
            console.log(instrumentation_info_list);
            done(null, null);
        });
    },
    function(done) {
        console.log('TEST: install');
        api.install_apk('/storage/external_storage/sda1/test_cases/CtsBluetoothTestCases.apk', function(err) {
            console.log('TEST: install result: ' + err);
            done(null, null);
        });
    },
    function(done) {
        console.log('TEST: run test');
        api.run_test('com.android.cts.bluetooth', 'android.test.InstrumentationCtsTestRunner', function(err) {
            done(null, null);
        });
    }
], function(err) {
    if (err) {
        console.log('err: ' + err);
    }
    process.exit();
});