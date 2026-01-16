const assert = require('chai').assert;
const myApp = require('../src/Aritgeo.js');
describe("Arithmetic or Geometric", function(){

	describe("handle invalid input", function(){
		it ("should return error for [m,n,2]}", function(){
			assert.equal(myApp.aritGeo([m,n,2]), 'Array of numbers is required');
		});
	})

	describe("handle neither Arithmetic nor Geometric progression", function(){
		it ("should return -1 for [1,2,5,6,9]", function(){
			assert.equal(myApp.aritGeo([1,2,5,6,9]), '-1');
		});
	})

	describe("handle neither Arithmetic nor Geometric progression", function(){
		it ("should return -1 for [2,4,8,16,17,34]", function(){
			assert.equal(myApp.aritGeo([2,4,8,16,17,34]), '-1');
		});
	})

	describe("handle Arithmetic progression", function(){
		it ("should return Arithmetic for [3,6,9,12,15]}", function(){
			assert.equal(myApp.aritGeo( [3,6,9,12,15]), 'Arithmetic');
		});
	})

	describe("handle Arithmetic progression", function(){
		it ("should return Arithmetic for [5,10,15,20]}", function(){
			assert.equal(myApp.aritGeo( [5,10,15,20]), 'Arithmetic');
		});
	})

	describe("handle Geometric progression", function(){
		it ("should return Geometric for [6,12,24,48,96]}", function(){
			assert.equal(myApp.aritGeo([6,12,24,48,96]), 'Geometric');
		});
	})

	describe("handle Geometric progression", function(){
		it ("should return Geometric for [3,9,27]}", function(){
			assert.equal(myApp.aritGeo([3,9,27]), 'Geometric');
		});
	})

	describe("handle empty Array", function(){
		it ("should return 0 for []}", function(){
			assert.equal(myApp.aritGeo([]), '0');
		});
	})

	describe("handle invalid input", function(){
		it ("should return error for [' ', 'tyu']", function(){
			assert.equal(myApp.aritGeo([" ", "tyu"]), 'Array of numbers is required');
		});
	})

	describe("handle invalid input", function(){
		it ("should return error for '1,2,3,4']}", function(){
			assert.equal(myApp.aritGeo('1,2,3,4'), undefined);
		});
	})
})