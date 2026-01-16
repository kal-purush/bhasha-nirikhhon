import { describe, it } from "mocha";
import assert from "assert";

describe("Try mocha async test", function() {
    it("This is a negative test.", function(done) {
        const testPromise = new Promise(function(resolve, reject) {
            setTimeout(function() {
                resolve("Hello!");
            }, 200);
        });
        testPromise
            .then(function(result) {
                assert.equal(result, "Hello, world!");
                done();
            }, done)
            .catch(function(error) {
                console.log("The test failed as expected.");
                done(error);
            });
    });

    it("This is a negative test as well.", function() {
        const testPromise = new Promise(function(resolve, reject) {
            setTimeout(function() {
                resolve("Hello!");
            }, 200);
        });

        return testPromise.then(function(result) {
            assert.equal(result, "Hello, world!");
        });
    });

    it("This is another negative test.", async function() {
        const testPromise = new Promise(function(resolve, reject) {
            setTimeout(function() {
                resolve("Hello!");
            }, 200);
        });

        const result = await testPromise;
        assert.equal(result, "Hello, world!");
    });
});