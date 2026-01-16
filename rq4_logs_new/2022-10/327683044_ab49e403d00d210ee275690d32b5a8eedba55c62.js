let arr = [1, 4, 5, 6, 5, 23, 2];

let targetSum = 25

let ans = [];

let ind = 0;
// console.log(a);
for(let i = 1; i < arr.length; i++) {
    
    let a = targetSum - arr[ind];
    // console.log(arr[ind]);
    // console.log("this is a " + a);
    // console.log("ind --> " + ind);
    // console.log ("i --> " + arr[i]); 
    
    if(a == arr[i]) {
        // ans.push(arr[ind]);
        // ans.push(arr[i]);
        // console.log(arr[ind], a);
        // console.log(arr[ind]);

        ans.push(arr[ind]);
        ans.push(a);
    }

    ind++;   
}


console.log(ans);




