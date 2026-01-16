// Warmup tests
function compareTriplets(a, b) {
  let countA = 0
  let countB = 0
  for( let i = 0;i < a.length;i++ ){
      if(a[i]>b[i]) countA++ 
      if(a[i]<b[i]) countB++ 
  }
return [countA, countB]
}