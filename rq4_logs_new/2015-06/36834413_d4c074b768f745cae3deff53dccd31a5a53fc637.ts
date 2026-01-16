
function range(high, fn) {
	return Array.apply(0, Array(high)).map((_,i)=>fn(i))
}

function normalize(data : number[][]) {
	var zeros = range(data[0].length, _ => 0)	
	var reduce = (data, fn) => data.reduce((p, c) => p.map(
		(_,i) => fn(p[i], c[i])), zeros)
		.map(s => s / data.length)
	var map = (data, fn) => data.map(row => row.map(fn))
	
	var μ = reduce(data, (p, c) => p + c)
	var zeroed = map(data, (c, i) => c - μ[i])
	var σ = reduce(zeroed, (p, c) => p + c * c).map(Math.sqrt)
	return [map(zeroed, (c, i) => c == 0 ? 0 : c / σ[i]), μ, σ]
}

// TypeScript multivariable logistic regression using gradient descent
// - y is 0 or 1
// by Jomo Fisher
function logistic(y : number[], x : number[][], iterations : number) {
	var [xnorm, μ, σ] = normalize(x)
	x = xnorm.map(a=>[1].concat(a))

	var m = x[0].length
	var sum = f => y.reduce((s,_,i) => s + f(i), 0)
	var Θ = range(m, _ => 0)
	var h = i => 1 / Math.exp(-Θ.reduce((p, c, j) => p += c * x[i][j], 0))
	var α = 0.01 / m
	
	range(iterations, _ => 
		Θ = Θ.map((Θj, j)=> 
			Θj - α * sum(i=>(h(i) - y[i]) * x[i][j])))
			
	return (...arr) => 1 / Math.exp(-arr.reduce((p, c, j) => 
		p + Θ[j + 1] * (c - μ[j]) / σ[j], Θ[0]))
}

// Training data: square feet, number of bedrooms, price
var housing = [[2104,3,399900],[1600,3,329900],[2400,3,369000],[1416,2,232000],[3000,4,539900],[1985,4,299900],[1534,3,314900],[1427,3,198999],[1380,3,212000],[1494,3,242500],[1940,4,239999],[2000,3,347000],[1890,3,329999],[4478,5,699900],[1268,3,259900],[2300,4,449900],[1320,2,299900],[1236,3,199900],[2609,4,499998],[3031,4,599000],[1767,3,252900],[1888,2,255000],[1604,3,242900],[1962,4,259900],[3890,3,573900],[1100,3,249900],[1458,3,464500],[2526,3,469000],[2200,3,475000],[2637,3,299900],[1839,2,349900],[1000,1,169900],[2040,4,314900],[3137,3,579900],[1811,4,285900],[1437,3,249900],[1239,3,229900],[2132,4,345000],[4215,4,549000],[2162,4,287000],[1664,2,368500],[2238,3,329900],[2567,4,314000],[1200,3,299000],[0852,2,179900],[1852,4,299900],[1203,3,239500]]

var f = logistic(
	housing.map(a=>a[2] > 500000 ? 1 : 0),
	housing.map(a=>a.slice(0, 2)),
	40000)
console.log(f(2104,3))




