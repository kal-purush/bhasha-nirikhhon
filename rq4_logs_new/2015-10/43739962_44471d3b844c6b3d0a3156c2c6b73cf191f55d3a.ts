import obs = require('../src');
import chai = require('chai');

var expect = chai.expect;

describe('observable object tests', () => {

    var object = obs.observe('value');

    it('will return a function', () => {
        expect(typeof object).to.equal('function');
    });

    it('will accept a subscriber', () => {
        expect(object.subscribe.bind(object.subscribe, (x => typeof x === 'string'))).to.not.throw;
    });

    it('will not accept a subscriber that is not a function', () => {
        expect(object.subscribe.bind(object.subscribe, 'not a function')).to.throw;
    });

    it('will notify subscribers', done => {
        object.removeSubscribers();
        object.subscribe(val => {
            expect(val).to.equal('newvalue');
            done();
        });
        object('newvalue');
    });

    it('will return the current value', () => {
        expect(object()).to.equal('newvalue');
    });
});

describe('observable array tests', () => {

    var object = obs.observeArray<string>([]);

    beforeEach(() => {
        object.removeSubscribers();
    });

    it('will return a function', () => {
        expect(typeof object).to.equal('function');
    });

    it('will accept a subscriber', () => {
        expect(object.subscribe.bind(object.subscribe, (x => null))).to.not.throw;
    });

    it('will not accept a subscriber that is not a function', () => {
        expect(object.subscribe.bind(object.subscribe, 'not a function')).to.throw;
    });

    it('will not accept a non-array type', () => {
        expect(object.bind(object, 'not a function')).to.throw;
    });

    it('will accept an array type', () => {
        expect(object.bind(object, ['1', '2', '3'])).to.not.throw;
    });

    it('will notify subscribers', done => {
        object.subscribe(x => {
            expect(x.join('')).to.equal('abc');
            done();
        });

        object(['a', 'b', 'c']);
    });

    it('will pop and notify', done => {
        object(['a', 'b', 'c']);
        object.subscribe(x => {
            expect(x.join('')).to.equal('ab');
            done();
        });

        expect(object.pop()).to.equal('c');
    });

    it('will push and notify', done => {
        object(['a', 'b', 'c']);

        object.subscribe(x => {
            expect(x.join('')).to.equal('abcd');
            done();
        });

        expect(object.push('d')).to.equal(4);
    });

    it('will shift and notify', done => {

        object.subscribe(x => {
            expect(x.join('')).to.equal('bcd');
            done();
        });

        expect(object.shift()).to.equal('a');

    });

    it('will unshift and notify', done => {

        object.subscribe(x => {
            expect(x.join('')).to.equal('azbcd');
            done();
        });

        expect(object.unshift('a', 'z')).to.equal(5);
    });

    it('will find a value', () => {
        expect(object.find(x => x === 'a')).to.equal('a');
    });

    it('will filter for a value', () => {
        expect(object.filter(x => x < 'c').join('')).to.equal('ab');
    });

    it('will replicate .some', () => {
        expect(object.some(x => x === 'q')).to.be.false;
        expect(object.some(x => x === 'z')).to.be.true;
    });

    it('will replicate .every', () => {
        expect(object.every(x => x <= 'z')).to.be.true;
        expect(object.every(x => x === 'z')).to.be.false;
    });

});