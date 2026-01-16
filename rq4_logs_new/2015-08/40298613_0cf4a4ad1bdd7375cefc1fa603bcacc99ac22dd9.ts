/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/chai/chai.d.ts" />

import chai = require('chai');
import {leaks, createDisposable, Disposable, Reference} from "../lib/h5flux";
var expect = chai.expect;

describe('disposable', () => {

    var initial_leaks: number;
    beforeEach(function() {
        initial_leaks = leaks();
    });

    afterEach(function() {
        expect(leaks(), 'leaks').to.be.equal(initial_leaks);
    });

    it('addRef / leak', (done) => {

        interface Data extends Reference {
            x: number
        }

        var count = 0;

        var disposable = createDisposable(creator);
        expect(count, 'count').to.deep.equal(0);
        expect(disposable.refCount(), 'refcount').to.deep.equal(0);

        var ref1 = disposable.addRef();
        expect(count, 'count').to.deep.equal(1);
        expect(disposable.refCount(), 'refcount').to.deep.equal(1);
        expect(ref1.x).to.deep.equal(100);
        done();
        initial_leaks++;

        function creator() {
            count++;
            return {
                instance: <Data>{ x: 100 },
                destructor: () => {
                    count--;
                }
            }
        }
    });

    it('addRef/releaseRef', (done) => {

        interface Data extends Reference {
            x: number
        }

        var count = 0;

        var disposable = createDisposable(creator);
        expect(count, 'coun1t').to.deep.equal(0);
        expect(disposable.refCount(), 'refcount1').to.deep.equal(0);

        var ref1 = disposable.addRef();
        expect(count, 'count2').to.deep.equal(1);
        expect(disposable.refCount(), 'refcount2').to.deep.equal(1);
        expect(ref1.x).to.deep.equal(100);

        ref1.releaseRef();
        setTimeout(() => {
            expect(count, 'count3').to.deep.equal(0);
            expect(disposable.refCount(), 'refcount3').to.deep.equal(0);

            done();
        }, 12);
        function creator() {
            count++;
            return {
                instance: <Data>{ x: 100 },
                destructor: () => {
                    count--;
                }
            }
        }

    });

    it('addRef/releaseRef two', (done) => {

        interface Data extends Reference {
            x: number
        }

        var count = 0;

        var disposable = createDisposable(creator);
        expect(count, 'count1').to.deep.equal(0);
        expect(disposable.refCount(), 'refcount1').to.deep.equal(0);

        var ref1 = disposable.addRef();
        expect(count, 'count2').to.deep.equal(1);
        expect(disposable.refCount(), 'refcount2').to.deep.equal(1);
        expect(ref1.x).to.deep.equal(100);

        var ref2 = disposable.addRef();
        expect(count, 'count3').to.deep.equal(1);
        expect(disposable.refCount(), 'refcount3').to.deep.equal(2);
        expect(ref2.x).to.deep.equal(100);

        ref2.x++;
        expect(ref1.x).to.deep.equal(101);

        ref1.releaseRef();
        setTimeout(() => {
            expect(count, 'count4').to.deep.equal(1);
            expect(disposable.refCount(), 'refcount4').to.deep.equal(1);
            expect(ref2.x).to.deep.equal(101);

            ref2.releaseRef();
            setTimeout(() => {
                expect(count, 'count5').to.deep.equal(0);
                expect(disposable.refCount(), 'refcount5').to.deep.equal(0);
                done();
            }, 12)
        }, 12)

        function creator() {
            count++;
            return {
                instance: <Data>{ x: 100 },
                destructor: () => {
                    count--;
                }
            }
        }
    });



});