/*
Lesson 0x05: using Bluebird promises

Bluebird promises are organized differently from Async.
A function that cannot complete immediately simply returns
a promise object. When the promised function is complete
(or "resolved"), it'll trigger the follow-on action.


this outputs:
-------------
resolving promise
promise starting
promise end <-- but this returns out of sequence
promise then 1
promise then 2
promise then 3
promise then 3.1
promise then 4

*/

var Promise = require('bluebird'),
	_ = require('lodash');

/**
 *	Returns a promise that resolves in a random amount of time
 */
function randomDelay() {
	var delay = Math.floor(Math.random()*1000);
	return new Promise((resolve, reject) => {
		setTimeout(resolve(), delay);
	});
}


console.log('promise starting');

// randomDelay returns a promise
randomDelay().then(function() {

	// the follow-on action is a simple function. Once
	// this is complete the next follow-on action starts.
	console.log('promise then 1');

}).then(function(done) {

	// this action returns a new promise. The next action
	// starts when this new promise is resolved.
	console.log('promise then 2');
	return randomDelay();

}).then(function() {

	// this action returns a new promise that itself
	// returns a promise. The next action starts when
	// this inner promise is resolved.
	console.log('promise then 3');
	return randomDelay().then(function () {
		console.log('promise then 3.1');
	});
}).then(function() {

	// the last of the chain.
	console.log('promise then 4');
});

// now we're outside the promise chain, so this is executed
// immediately after the randomDelay method returns its 
// (unresolved) promised.
console.log('promise end <-- but this returns out of sequence');





