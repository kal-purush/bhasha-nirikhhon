// Given an array of integers nums and an integer target, return indices of the two numbers such that they add up to target.
let module = require('./data')
let nums= module.numbersArray;
let target= module.target;

// Two adjacent numbers
var twoAdjacentNumbers = function(nums, target) {
    return nums.reduce((pre,number, index)=> {
        if(Array.isArray(pre)) return pre;
        if(pre+number=== target) 
            return [pre, number]
        else return pre+=nums[index]

    })
}
console.log(twoAdjacentNumbers(nums, target));

// Two numbers from an array
const twoNumbersSum = function(nums, target) {
    const obj = {};
    for(let i=0; i<nums.length; i++){
        if(obj[nums[i]]>=0){ // If the index is gt or e to 0, and not undefine..
            return [ obj[nums[i] ] , i]
        }
        obj[target-nums[i]] = i
    }
};
console.log(twoNumbersSum(nums, target));


// Get the longest substring lenght with distinct numbers
var lengthOfLongestSubstring = function(str) {
    let maxCount=0; let s=''; 
    for(c of str){
        if(s ==='') s=c;
        else if(s.indexOf(c) === -1) s+=c;
        else{
            if(maxCount< s.length) maxCount=s.length;
            s=s.substring(s.indexOf(c)+1) + c;
        }  
    }

    return (maxCount< s.length) ? s.length : maxCount 
};
// Another option
// var lengthOfLongestSubstring = function(s) {
//     var last = {};
//     var start = 0;
//     var longest = 0;
//     s = s.split(''); // Each char into array
//     s.forEach((item, index) => {
//         if(last[item] >= start)
//             start = last[item] + 1;
//         longest = Math.max(longest, index-start+1);
//         last[item] = index;
//     });
//     return longest;
// };

console.log(lengthOfLongestSubstring("pwwkew")); 

 
// Give the max sum of unique sub arrays.
var maximumUniqueSubarray = function(nums) {
    let maxSum=0;
    let arr=[]; 
    let arraySum=0;
for(c of nums){
     if(!arr.length) {
         arr.push(c)
        }
     else if(!arr.includes(c)) arr.push(c);
     else{
         arraySum= arr.reduce((sum, number) => sum+=number ,0);
         if(maxSum< arraySum) maxSum=arraySum;
         arr= arr.splice(arr.indexOf(c)+1, arr.length)
        arr.push(c);
     }  
 }
arraySum= arr.reduce((sum, number) => sum+=number ,0);
 return (maxSum< arraySum) ? arraySum : maxSum 
};

// var maximumUniqueSubarray = function(N) {
//     let a = new Int8Array(10001), sum = max = 0, len = N.length
//     for (let l = 0, r = 0; r < len; r++) {
//         a[N[r]]++, sum += N[r]
//         while (a[N[r]] > 1) a[N[l]]--, sum -= N[l++]
//         max = sum > max ? sum : max
//     }
//     return max
// };
console.log(maximumUniqueSubarray(nums)); 


// Set
const myArray=[1,2,3,4,5,5,5,6];
const mySet= new Set(myArray); // Set {1,2,3,4,5,6}
const setConvertToArray= [...mySet] // Gives an array from the set. [1,2,3,4,5,6]

const myMap= new Map([['name', 'shira'],['lastName', 'hachmon']])
myMap.set('a', 'b')
console.log(myMap);