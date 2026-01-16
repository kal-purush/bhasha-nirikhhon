// 1. function test num prime or not
// 2. start from 2,end with number-1;test reminder.
const primeTest = function(num){
    if(num === 2){return true}
    for(let i =2; i<num; i++){
        if(num%i===0){return false}
    }
    return true
}

const primeFac = function(num){
    const results = [];
    for (let i=2; i<num; i++){
        if(primeTest(i)&&(num%i ===0)){
            results.unshift(i);
            console.log(i);
        }
    }
    console.log(results[0]);
    return results;
}

primeFac(59569);