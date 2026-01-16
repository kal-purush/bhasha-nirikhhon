/// <reference path="range.ts"/>
/// <reference path="shuffle.ts"/>
var percent = 0.5

function runs() {
var pop = 100
	var indices = range(pop, i => i)
	var set = range(pop, i => false)
	var seen = 0
	var r = 0
	while (seen < pop) {
		shuffle(indices)
		range(Math.round(percent * pop), i=> {
			if (set[indices[i]] === false) {
				++seen 
				set[indices[i]] = true
			}
		})
		++r
	}
	return r
}

function epochs() {
	var count = 5000
	var result = range(count, i=>runs())
	result.sort()
	console.log("90% interval is [%s, %s] days",
		result[Math.round(count * 0.05)] / 4,
	    result[Math.round(count * 0.95)] / 4)
}

epochs()
