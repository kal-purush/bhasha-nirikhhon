/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/chai/chai.d.ts" />

import chai = require('chai');
import {asap, asap_once, leaks} from "../lib/h5flux";

var expect = chai.expect;

describe('actions', () => {

    beforeEach(function(done) {
        asap(() => {
            expect(leaks(), 'before each leaks').to.be.equal(0);
            done();
        })
    });

    afterEach(function(done) {
        asap(() => {
            expect(leaks(), 'after each leaks').to.be.equal(0);
            done();
        })
    });

    it('asap', (done) => {
        var did: number[] = [];
        [1, 2, 3, 4, 5, 6].map((v) => {
            asap(() => {
                expect(did).to.not.contain(v);
                did.push(v);
                if (did.length == 7)
                    done();
            })
        })
        did.push(0);
        expect([0]).to.deep.equal(did);
    });

});