let val;

const list = document.querySelector('ul');
const listitems = document.querySelector('li:first-child');

val = listitems;
val = list;
val = list.childNodes;
console.log(val);
// console.log(typeof val); 
function dosome(arg) {
    let option = arg || 'default';
    console.log(option);
}
dosome();
dosome('Lo');
dosome(10);
/*ARRAY*/
const arr = [10, 20, 30];
console.log(arr);
console.log(arr.length);
arr.length = 10;
console.log(arr);
console.log(arr.length);
for (let i = 0; i < arr.length; i++) {
    console.log(arr[i]);
}
/*有關於的陣列的length特性式可讀也可寫的!*/
/*而改變陣列長度後，而多出來的索引位置都是undefined*/
/*那如果先變短在遍嘗呢?*/
const arr2 = new Array(10, 20, 30);
for (let i = 0; i < arr2.length; i++) {
    console.log(arr2[i]);
}
arr2.length = 2;
arr2.forEach(function (nm) {
    console.log(nm);
})
arr2.length = 3;
for (let i = 0; i < arr2.length; i++) {
    console.log(arr2[i]);
}
arr2.forEach(function(nm) {
    console.log(nm);
})
/*arr2的length變短在變長後，原本索引2位置的數也回不來囉!!*/
/*在Javascript裡面沒有實際的陣列，ARRAY只是用物件來模擬出相似的操作外觀*/
/*而索引只是代表數字的特性而已，所以我們也可以*/
console.log(arr2['0']);
/*像這個樣子使用字串來指定，只要它代表數字即可*/



const cater = {
    name: 'Joe',
    age: 22,
    toString: function () {
        return 'name:' + this.name + '  age:' + this.age;
    }
}
// console.log(cater.toString());
// console.log(cater.age);
// console.log(typeof cater.age);
/*傳入的引數比參數少的時候，參樹就是undefined*/
function ab(a, b) {
    // console.log(a);
    // console.log(b);
}
ab(10, 20);
ab(10);
ab();
ab(10, 20, 30, 40);
/*傳入比參數多的引數也是可行的，那麼多的引數會到哪裡呢? */
/*其實在函式function裡面，會自動宣告arguments名稱餐靠到陣列外觀物件*/
/*上頭帶有所有傳入的引數，所以也可以寫下面這個樣子*/
function sum() {
    let sum = 0;
    for (let i = 0; i < arguments.length; i++) {
        sum += arguments[i];
    }
    return sum;
}
console.log(sum(50, 10, 29, -3));
/*但注意，arguments不是array實例，而是具有數字作為特性，特性參考至傳入的引數*/
/*並具有length的特性代表傳入引數的個數*/

/*參數不過是具名的引數，如果改變參數值的話*/
function func(a, b) {
    console.log('a: ' + arguments[0]);
    console.log('b: ' + arguments[1]);
    a = 0;
    b = 0;
    console.log("a: " + arguments[0]);
    console.log("b: " + arguments[1]);
}
func(1, 2);
/*auguments索引對應的值也會跟著改變*/

/*由於傳入的引數不一定是等於參數個數，所以可以使用arguments來檢查*/
function check(a, b) {
    if (arguments.length != 2) {
        console.log('must give 2 argurments');
    }
}
check(1, 2);
check(1, 2, 3, 4);
/*而在實際上，其實很少檢查引數個數，而是在引數不足時提供預設值比較多*/
function numberGenerator(length, begin, step) {
    begin = begin || 0;
    step = step || 1;
    let numbers = [begin];
    for (let i = 0; i < (length - 1); i++) {
        numbers.push(numbers[i] + step);
    }
    return numbers;
}
console.log(numberGenerator(3));
console.log(numberGenerator(3, 5));
console.log(numberGenerator(3, 5, 3));
console.log(numberGenerator());
/*在選項非常多的時候可以使用 選項物件(Option Object)*/
function funcop(option) {
    if (arguments.length == 0) {
        console.log('please give object');
    } else {
        option = {
            x: option.x || 10,
            y: option.y || 20,
            z: option.z || 30
        };
        console.log(option.x);
        console.log(option.y);
        console.log(option.z);
    }
}
const option = {
    x: 12,
    y: 32
};
funcop(option);
funcop();
funcop({x:100,y:31});
/*呼叫函式時必須提供物件，物件上帶有函式所需的資料，函式內部對於物件沒有該資料時會給予預設值*/

let fnc = function(a,b) {
    return a+b;
}
console.log(fnc(1,2));
function sub(a,b) {
    return a - b;
}
let subnum = sub(1,1);
console.log(subnum);
console.log(subnum == false);
console.log(subnum === false);

(function(a,b) {
    console.log('a:' + a);
    console.log('b:' + b);
}) (1,2);

const obj = {
    '0' : 1,
    '1' : 2,
    '2' : 4,
    length : 3,
    // foeEach: function(callback) {
    //     for(let i =0;i < this.length;i++) {
    //         callback[this[i]];
    //     }
    // }
};
// obj.forEach(function(element) {   // run 不出
//     console.log(element);
// });
// arr2.forEach(function(element) {
//     console.log(element);
// });