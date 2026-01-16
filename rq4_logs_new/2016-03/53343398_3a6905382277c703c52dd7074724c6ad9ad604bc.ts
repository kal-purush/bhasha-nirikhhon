console.log("Hello World!");

let favNum: any = prompt("What your favorite number?");

favNum = parseInt(favNum);

if (isNaN(favNum)){
  alert("Invalid Input");
}
else{
  alert(favNum * 100);
}


let products = [
    { name:  'Cheese', price:  7.00 },
    { name:  'Milk', price:  3.00 },
    { name:  'Wine', price:  97.30 },
    { name:  'Grapes', price:  4.00 }
];

let sum = products.reduce(function (memo, ele, idx) {
    return ele.price + memo;
}, 0);

console.log(sum);