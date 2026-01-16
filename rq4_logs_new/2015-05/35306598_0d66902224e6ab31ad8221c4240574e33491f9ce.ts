declare function require(name:string);
declare function describe(what:string, how: () => void);
declare function it(what:string, how: (done?:(err?:any) => void) => void);
var assert = require('assert');
var request = require('request');
describe('Checkout', function () {
	[
		{ items: 'A', total: 50 },
		{ items: 'B', total: 70 },
		{ items: 'AAA', total: 120 },
		{ items: 'AAAAAA', total: 240 },
		{ items: 'C', total: 40 },
		{ items: 'CC', total: 70 }
	].forEach(function (c: any) {
		it('prices items correctly: ' + c.items, function (done){
			request
				.post('http://localhost:3000',
					{form: {items: c.items}},
					function (err, res){
						if (err) { return done(err); }
						assert.equal(res.body, c.total);
						done();
					});
		});
	});
});