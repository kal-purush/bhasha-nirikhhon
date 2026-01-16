var testMethod = () => {
    var x = 1;
    const inner = () => {
        return x;
    }
    var x = 99;
    return inner;
}

// var testMethod2 = () => {
//     let x = 1;
//     const inner = () => {
//         return x;
//     }
//     let x = 99;
//     return inner;
// }



console.log(testMethod()());
// console.log(testMethod2());