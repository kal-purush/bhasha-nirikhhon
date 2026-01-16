/// <reference path="../type_declarations/DefinitelyTyped/mocha/mocha.d.ts" />
/// <reference path="../type_declarations/DefinitelyTyped/node/node.d.ts" />
import assert = require('assert');

import base64 = require('../base64');

const wikipedia_leviathan_decoded = 'Man is distinguished, not only by his reason, but by this singular passion from other animals, which is a lust of the mind, that by a perseverance of delight in the continued and indefatigable generation of knowledge, exceeds the short vehemence of any carnal pleasure.'
const wikipedia_leviathan_encoded = 'TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdlLCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=';

describe('Test base64 encoding', () => {
  it(`should encode wikipedia example`, () => {
    var actual = base64.encodeStringToString(wikipedia_leviathan_decoded);
    assert.equal(actual, wikipedia_leviathan_encoded);
  });
});

describe('Test base64 decoding', () => {
  it(`should decode wikipedia example`, () => {
    var actual = base64.decodeStringToString(wikipedia_leviathan_encoded);
    assert.equal(actual, wikipedia_leviathan_decoded);
  });
});