function shortestToChar(S, C) {
	let counter = Number.POSITIVE_INFINITY;
	let res = [];
	for (let i = 0; i < S.length; i++) {
		if (S[i] === C) {
			counter = 0;
		} else {
			counter++;
		}
		res.push(counter);
	}
	counter = Number.POSITIVE_INFINITY;
	for (let i = S.length - 1; i >= 0; i--) {
		if (S[i] === C) {
			counter = 0;
		} else {
			counter ++;
		}
		res[i] = Math.min(counter, res[i]);
	}
	return res;
} 
let S = 'loveleetcode';
let C = 'e'
shortestToChar(S, C)