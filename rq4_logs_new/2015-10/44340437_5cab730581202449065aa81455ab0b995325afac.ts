/// <reference path="../../../typings/tsd.d.ts" />
import 'source-map-support/register';
import assert = require('assert');

import {GetObjectFromName, IsNullOrUndefined, IsNotNullnorUndefined} from '../../../src/lib/node/index';

describe("JsObjectUtil:", () => {
    context('Verify get object API methods', () => {
        it("(Native Object): get object from string notation", () => {
            var processOut = GetObjectFromName("process.stdout");
            assert.strictEqual(processOut, process.stdout, "should be the same object.");
        });

        it("(Module Object): get object from string notation and module path, with 2 layer deep", () => {
            var obj = GetObjectFromName("bunyan.stdSerializers1", "bunyan");
            assert.ok(IsNotNullnorUndefined(obj), "got object should not be undefined or null.");
            var bunyan = require('bunyan');
            var target = bunyan.stdSerializers;
            assert.ok(IsNotNullnorUndefined(target), "bunyan bundled serializer should not be undefined.");
            assert.strictEqual(obj, target, "should be the same object.");
        });

        it("(Module Object): get object from string notation and module path, with 3 layer deep", () => {
            var obj = GetObjectFromName("bunyan.stdSerializers.req", "bunyan");
            assert.ok(IsNotNullnorUndefined(obj), "got object should not be undefined or null.");
            var bunyan = require('bunyan');
            var target = bunyan.stdSerializers.req;
            assert.ok(IsNotNullnorUndefined(target), "bunyan bundled serializer should not be undefined.");
            assert.strictEqual(obj, target, "should be the same object.");
        });
    });

    context('Verify check Null, Undefined object methods', () => {
        it("Verify check object null or undefined API methods", () => {
            var nullObj = null;
            var normalObj = {};
            var undefinedObj = normalObj['not exist property'];

            assert.ok(IsNullOrUndefined(nullObj) === true, "should return false when input argument is null.");
            assert.ok(IsNullOrUndefined(undefinedObj) === true, "should return false when input argument is undefined.");
            assert.ok(IsNullOrUndefined(normalObj) === false, "should return true when input argument is normal object");

            assert.ok(IsNotNullnorUndefined(nullObj) === false, "should return false when input argument is null.");
            assert.ok(IsNotNullnorUndefined(undefinedObj) === false, "should return false when input argument is undefined.");
            assert.ok(IsNotNullnorUndefined(normalObj) === true, "should return true when input argument is normal object");

        });
    });
});