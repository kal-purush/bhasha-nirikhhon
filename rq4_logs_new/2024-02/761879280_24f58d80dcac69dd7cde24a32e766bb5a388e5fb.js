// - створити функцію яка обчислює та повертає площу прямокутника зі сторонами а і б
// S = a · b,
function squareRectangle(a, b) {
    return a * b;
}

console.log(squareRectangle(10, 20));
// - створити функцію яка обчислює та повертає площу кола з радіусом r
// S = πr2
function squareCircle(radius) {
  //  return 3.14 * (radius * radius);
     return Math.PI*Math.pow(radius, 2)

}

console.log(squareCircle(10));
// - створити функцію яка обчислює та повертає площу циліндру висотою h, та радіутом r
// 2πR(H + R)
function squareCylinder(radius, height) {
    return 2 * Math.PI * radius * (height + radius);
}

console.log(squareCylinder(10,20))

// - створити функцію яка приймає масив та виводить кожен його елемент
function arrs (arr) {
    for (const arrElement of arr) {
        console.log(arrElement);

    }

}

let mass = [11, 22, 33, 'string', true, 111];
   console.log(arrs(mass));

// - створити функцію яка створює параграф з текстом.
// Текст задати через аргумент
function create(text) {
    document.write(`<div>
<P>${text}</P>
</div>`)
}

console.log(create('How do you do?'));

// - створити функцію яка створює ul з трьома елементами li.
// Текст li задати через аргумент всім однаковий

function createLi3(text){
    document.write(`<ul>

         <li>${text}</li>
         <li>${text}</li>
         <li>${text}</li>
</ul>`)
}

console.log(createLi3('loren'))

// - створити функцію яка створює ul з трьома елементами li.
// Текст li задати через аргумент всім однаковий. 
// Кількість li визначається другим аргументом, який є числовим (тут використовувати цикл)

function createLiWithArgument(text,namberLi){
    document.write(` <ul> `)

    for (let i = 0; i < namberLi; i++) {
       document.write(`<li>${text}</li>`)
    }
    document.write(`</ul>`)

}

console.log(createLiWithArgument('hello',9))

// - створити функцію яка приймає масив примітивних елементів (числа,стрінги,булеві),
// та будує для них список


function list(arr){

    for (const arrElement of arr) {
        document.write(`
<ul>
<li>${arrElement}</li>
</ul>
`)

    }
}

console.log(list([11, 22,'hello', true]));


// - створити функцію яка приймає масив об'єктів з наступними полями id,name,age ,
// та виводить їх в документ. Для кожного об'єкту окремий блок.

function arrWithIdNameAge(arr) {
    for (const arrElement of arr) {
        document.write(`<div>id: ${arrElement.id}, Name: ${arrElement.name}, Age: ${arrElement.age}</div>`);


    }

}

let users = [
    {id:1,name: 'vasya', age: 31},
    {id:2,name: 'petya', age: 30},
    {id:3,name: 'kolya', age: 29},
    {id:4,name: 'olya', age: 28},
    {id:5,name: 'max', age: 30},
    {id:6,name: 'anya', age: 31},
    {id:7,name: 'oleg', age: 28},
    {id:8,name: 'andrey', age: 29},
    {id:9,name: 'masha', age: 30 },
    {id:10,name: 'olya', age: 31 },
    {id:11,name: 'max', age: 31}
];
console.log(arrWithIdNameAge(users))

// - створити функцію яка повертає найменьше число з масиву
let namber = [11, 22, 33, 4, 66, 78, 9, 3];
function minNamber(arr){
    let min=arr[0]
    for (const arrElement of arr) {
        if (min>arrElement) {
            min=arrElement
        }
    } return min
}

console.log(minNamber(namber))
// - створити функцію sum(arr)яка приймає масив чисел,
// сумує значення елементів масиву та повертає його.
// Приклад
let sum=([1,2,10]) //->13
function summ (arr){
    let resalt=0
    for (const arrElement of arr) {
       resalt=resalt+arrElement

    } return resalt
}

console.log(summ(sum));
// - створити функцію swap(arr,index1,index2).
// Функція міняє місцями заняення у відаовідних індексах
// Приклад  swap([11,22,33,44],0,1) //=> [22,11,33,44]
let swa=[11,22,33,44] //=> [22,11,33,44]
function swap(arr,index1,index2) {
    index1 = arr[0];
    index2=arr[1]
    arr[0] = index2;
    arr[1]=index1
    return arr
}

console.log(swap(swa,3,0))
// - Написати функцію обміну валюти exchange(sumUAH,currencyValues,exchangeCurrency)
// Приклад exchange(10000,[{currency:'USD',value:40},{currency:'EUR',value:42}],'USD') // => 250
function exchange(sumUAH,currencyValues,exchangeCurrency){
    let resultat= sumUAH/exchangeCurrency
return resultat  +` ${currencyValues}`
}

console.log(exchange(10000, 'USD', 40));