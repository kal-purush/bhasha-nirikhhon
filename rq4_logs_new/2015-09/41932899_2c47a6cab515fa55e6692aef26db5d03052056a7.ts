/// <reference path="../../typings/es6-promise/es6-promise.d.ts" />

function promiseMap<A, B>(promise: Promise<A>, callback: (A) => B): Promise<B> {
	return new Promise<B>((resolve, reject) => {
		promise.then(
			(value) => {
				resolve(callback(value));
			},
			reject
		);
	});
}


function getResolvedPromises<T>(promises: Promise<T>[]) {
	return new Promise<T[]>((resolve, reject) => {
		var resolved: T[] = [];
		
		var completed = 0;
		var total = promises.length;
		function checkComplete() {
			completed++;
			if (completed == total) {
				resolve(resolved);
			}
		}
		
		promises.map((promise) => {
			promise.then((value) => {
				resolved.push(value);
				checkComplete();
			}, checkComplete)
		})
	})
}

function positiveRace<T>(promises: Promise<T>[]) {
	return new Promise<T>((resolve, reject) => {
		
		var resolved: T[] = [];
		
		var completed = 0;
		var total = promises.length;
		var isCompleted = false;
		
		function checkComplete() {
			completed++;
			if (completed == total && !isCompleted) {
				reject();
			}
		}
		function complete(value) {
			if (!isCompleted) {
				resolve(value);
				isCompleted = true;
			}
			checkComplete();
		}
		
		promises.map((promise) => {
			promise.then(complete, checkComplete)
		})
	})
}