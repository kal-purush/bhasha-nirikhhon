/// <reference path="../../typings/tsd.d.ts" />

import assert = require("assert");
import Sentence = require("./sentence");

describe('Sentence', () => {
    describe("#parse()", () => {
        it('One sentence text should be converted to array of one sentence', () => {
            assert.equal(Sentence.parse("Be or not to be.")[0].toString(), "Be or not to be.")
        });
    });
});