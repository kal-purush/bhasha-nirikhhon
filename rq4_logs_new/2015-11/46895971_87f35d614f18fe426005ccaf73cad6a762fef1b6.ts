/// <reference path="../../typings/power-assert/power-assert.d.ts" />
/// <reference path="../../typings/mocha/mocha.d.ts" />

import assert from "power-assert";
import * as jax from "../../src/jax";



describe("url", () => {
    it("getQueryFromUrl", () => {
        assert.deepEqual({v : "ooMQdcTh3Y8"}, jax.url.getQueryFromUrl("https://www.youtube.com/watch?v=ooMQdcTh3Y8"));
    });
});