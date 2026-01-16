let findMaxString = (myStr) => {
    const maxStr = Math.max(...myStr.map(x=>x.length));
    console.log(myStr.filter(x=>x.length === maxStr));
}
findMaxString(["adb", "Isaka", "There's something", "short", "Make it long"]);

weAreAdding = (myArr) => {
//   const addedTotal = myArr.reduce((currentSum, myArr)=> {
//       return myArr + currentSum
//   },0);
//   console.log(addedTotal);

    const totalSum = myArr.reduce((currentTotal, myArr)=> {
        return myArr + currentTotal
    }, 0)
    console.log(totalSum)

}
weAreAdding([6,7,3,2,7,8,9,23,3]);