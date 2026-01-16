import assert = require('intern/chai!assert');
import useRoot = require('../../useRoot');
import registerSuite = require('intern!object');

var hasDOM:boolean = typeof document !== 'undefined';

registerSuite({
    name: 'useRoot',
    'basic': function () {
        if (!hasDOM) {
            this.skip('No DOM present');
        }
        var div = <HTMLElement>document.body.appendChild(document.createElement('div')),
            p = <HTMLElement>div.appendChild(document.createElement('p')),
            qsa = div.querySelectorAll;
        p.appendChild(document.createElement('span'));

        assert.equal(qsa.call(p, 'div span').length, 1);
        assert.equal(useRoot(p, 'div span', qsa).length, 0);
    },
    'with id': function () {
        if (!hasDOM) {
            this.skip('No DOM present');
        }
        var div = <HTMLElement>document.body.appendChild(document.createElement('div')),
            p = <HTMLElement>div.appendChild(document.createElement('p')),
            qsa = div.querySelectorAll;

        p.appendChild(document.createElement('span'));
        p.setAttribute('id', 'test_p');

        assert.equal(useRoot(p, 'div span', function (query:string) {
            assert.equal(p.id, 'test_p');
            return qsa.call(this, query);
        }).length, 0);
    },
    'without id': function () {
        if (!hasDOM) {
            this.skip('No DOM present');
        }
        var div = <HTMLElement>document.body.appendChild(document.createElement('div')),
            p = <HTMLElement>div.appendChild(document.createElement('p')),
            qsa = div.querySelectorAll;

        p.appendChild(document.createElement('span'));

        assert.strictEqual(p.id, '');

        assert.equal(useRoot(p, 'div span', function (query:string) {
            assert(p.id.length);
            return qsa.call(this, query);
        }).length, 0);

        assert.strictEqual(p.id, '');
    }
});