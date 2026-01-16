function range(high, fn) : any[] {
    return Array.apply(0, Array(high)).map((_,i)=>fn(i))
}

/* matrix(5, 1, Math.random)

  [ [ 0.9687679479829967,
      0.7161620596889406,
      0.97352325450629,
      0.06294197984971106,
      0.9319041615817696 ] ]
*/
function matrix(height, width, fn) : any[][] {
  return range(width, i=>range(height, j=>fn(i, j)))
}

function sigmoid(z) {
    return 1.0/(1.0+Math.exp(-z))
}
    
class Network {
  n : number;
  s : number[];
  b : number[][];
  w : number[][][];  
}

function network(sizes:number[]) {
  var result = new Network()
  result.s = sizes
  result.n = sizes.length
  result.b = range(sizes.length - 1, i=>range(sizes[i], Math.random))
  result.w = range(sizes.length - 1, i=>matrix(sizes[i], sizes[i+1], Math.random))
  return result
}

/*
  a.length is arbitrary
  a[n].length == b.length
*/
function dot(a:number[][], b:number[]) {
  var result = range(a.length, _=>0)
  for(var i=0; i<a.length; ++i) {
    for(var j=0; j<b.length; ++j) {
      result[i] += (a[i][j] * b[j])
    }
  }
  return result
}

function plus(a:number[], b:number[]) {
  return a.map((_,i)=>a[i] + b[i])
}

function feedforward(net : Network, a: number[]) {
  range(net.n - 1, i=>
    a = plus(dot(net.w[i], a), net.b[i])
          .map(sigmoid))
  return a;
}

var net = network([6, 5, 4, 3, 2, 1])
//console.log(net.w[0])
//console.log(net.b[0])
var a = range(net.s[0], Math.random)
//console.log(a)
//console.log("-----")
console.log(feedforward(net, a))
//console.log(net.b)