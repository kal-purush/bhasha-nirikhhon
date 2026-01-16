//ПРИКЛАД ССИЛОЧНИХ ЗМІННИХ
/* let data = {
    a: 17,
    b: 'Word'
}

let newData = data //НОВА ЗМІННА newData ЯКА ПОСИЛАЄТЬСЯ НА ПОПЕРЕДНЮ ЗМІННУ data
newData.c = false

console.log(newData.c) */



//ПРИКЛАД ЗМІНИ ТИПУ ЗМІННОЇ З ФУНКЦІЇ НА ЧИСЛО
/* function a() {
    console.log('Yo Hed')
}

a = 10
a() */



//ПРИКЛАД ПОМИЛКИ ПІД ЧАС ЗМІНИ ЗНАЧЕННЯ ЗМІННОЇ
/* const a = () => {
    console.log('Yo Hed')
}

a() // 'Yo Hed'
a = 10 //Appear the following error: TypeError: Assignment to constant variable.
a() */



//ВИКЛИК ВЛАСТИВОСТІ ОБ'ЄКТА myCity
/* const myCity = {
    city: 'Lviv',
    popular: true,
    country: 'Ukraine'
}

console.log(myCity.city) */



//ПРИКЛАД ПРИСВОЮВАННЯ ІНШОГО ЗНАЧЕННЯ ДЛЯ ВЛАСТИВОСТІ ОБ'ЄКТА myCity
/* const myCity = {
    city: 'Lviv',
    popular: true,
    country: 'Ukraine'
}

myCity.city = 'Kyiv'//Нове значення властивості city об'єкта myCity

console.log(myCity.city) */



//ДОДАВАННЯ НОВОЇ ВЛАСТИВОСТІ ДЛЯ ОБ'ЄКТА myCity
/* const myCity = {
    city: 'Lviv'
}
console.log(myCity)

myCity.popular = true //Додавання нової властивості popular у об'єкт myCity
myCity.country = 'Ukraine' //Додавання нової властивості country у об'єкт myCity
console.log(myCity) */



//ПРИКЛАД ВИДАЛЕННЯ ВЛАСТИВОСТІ У ОБ'ЄКТІ ІЗ ДОПОМОГОЮ ОПЕРАТОРА delete
/* const myCity = {
    city: 'Lviv',
    popular: true,
    country: 'Ukraine'
}

console.log(myCity)

delete myCity.popular //Видалення властивості popular із об'єкта myCity
console.log(myCity) */



//РОБОТА ІЗ ВЛАСТИВОСТЯМИ ОБ'ЄКТА ЗА ДОПОМОГОЮ КВАДРАТНИХ СКОБОК []
/* const myCity = {
    city: 'Lviv'
}

myCity['popular'] = true //Створення нової властивості з назвою popular і присвоєння їй значення true
console.log(myCity)

const countryProperName = 'country' //Створення змінної countryProperName, яка має значення country
myCity[countryProperName] = 'Ukraine' //Створення нової властивості, яка візьме значення із змінної countryProperName. А воно дорівнює country. І тоді присвоїть цій властивості country значення Ukraine
console.log(myCity) */



//ПРИКЛАД ВКЛАДЕНИХ ОБ'ЄКТІВ info, ЯК ВЛАСТИВІСТЬ У ІНШОМУ ОБ'ЄКТІ myCity
/* const myCity = {
    city: 'Lviv',
    info: {             //Об'єкт info є вкладенним об'єктом myCity. і в свою чергу info також має свої властивості
        isPopular: true,
        country: 'Ukraine'
    }
}

console.log(myCity.info.country) 

delete myCity.info.isPopular       //Приклад видалення властивості вкладенного об'єкта за допомогою крапки
delete myCity.info['country']       //Приклад видалення властивості вкладенного об'єкта за допомогою квадратних скобок []

console.log(myCity) */



//ПРИКЛАД ВИКОРИСТАННЯ ЗАВЧАСНО СТВОРЕННИХ ЗМІННИХ ДЛЯ ВИКОРИСТАННЯ ЇХ У ОБ'ЄКТІ
/* const userName = 'Hed'
const userAge = 30

const userProfile = {
    userName: userName,
    userAge: userAge,
    hasSigned: false
}

console.log(userProfile) */

//АНАЛОГІЧНИЙ ВАРІАНТ, АЛЕ З ВИКОРИСТАННЯМ СКОРОЧЕННОГО ЗАПИСУ ВЛАСТИВОСТЕЙ
/* const userName = 'Hed'
const userAge = 30

const userProfile = {
    userName,
    userAge,
    hasSigned: false
}

console.log(userProfile) */



//МЕТОДИ. Метод - це властивість об'єкта, значення якого це функція
/* const myCity = {
    city: 'Lviv',
    cityGreeting: function () {     //Позначення функції
        console.log('Greeting!')
    }
}

myCity.cityGreeting() */        //Виклик метода

//або ж:

/* const myCity = {
    city: 'Lviv',
    cityGreeting() {        //Позначення функції
        console.log('Greeting!')
    }
}

myCity.cityGreeting()  */      //Виклик метода



//СТВОРЕННЯ НОВОГО ОБ'ЄКТА НА ОСНОВІ ПОПЕРЕДНЬОГО

//Варіант 1:
/* const user = {      //Створення першого об'єкта
    userId: 12,
    userName: 'Bob'
}

const user2 = Object.assign({}, user)     //Створення другого об'єкта user2 на основі попереднього user
user2.userId = 23     //Зміна значення властивості userId у другому об'єкті user2. Але не зачепаючи першого об'єкта user

console.log(user.userId)
console.log(user2.userId) */


//Варіант 2:
/* const user = {      //Створення першого об'єкта
    userId: 12,
    userName: 'Bob'
}

const user2 = {...user}       //Створення другого об'єкта userTwo на основі попереднього user (... - це оператор розділення об'єкта на властивості)
user2.userName = 'Mike'       //Зміна значення властивості userName у другому об'єкті user2. Але не зачепаючи першого об'єкта user

console.log(user.userName)
console.log(user2.userName) */

//Варіант 3:
/* const user = {      //Створення першого об'єкта
    userId: 12,
    userName: 'Bob'
}

const user2 = JSON.parse(JSON.stringify(user))      //Створення другого об'єкта userTwo на основі попереднього user із допомогою парсування та розпарсування
user2.userName = 'Mike'

console.log(user.userName)
console.log(user2.userName) */



//ФУНКЦІЇ

/* let a = 5
let b = 3

function sum(a, b) {        //Створення функції sum, яка сумує два числа і виводить результат
    const c = a + b
    console.log(c)
}

sum(a, b)       //Виклик функції sum
a = 12
b = 8
sum(a, b)  */      //Виклик функції sum


//СТВОРЕННЯ ФУНКЦІЇ ЯКА ПОВЕРТАЄ ЗНАЧЕННЯ РЕЗУЛЬТАТУ ОБЧИСЛЕННЯ
/* function myFun(a, b) {      //Оголошення функції
    let c
    a = a - 1
    c = a + b
    return c        //Повертає значення с
}

a = 5
b = 2
console.log(myFun(a, b)) */        //Вивід результату роботи функції



//ВИКЛИК ФУНКЦІЇ !!!
/* function myFun(a, b) {      //Створення функції (a та b це змінні, яким буде присвоєне значення Аргументів (11 та 2) під час виклику функції)
    let c
    a = a - 1
    c = a + b
    return c        //Повертає значення с
}
//myFun(11, 2)     //Виклик функції myFun (11 та 2 це є Аргументи функції)
//console.log(myFun(11, 2))  //Виклик функції myFun і вивід результату у консоль
let result = myFun(11, 2)       //Присвоєння результату функції myFun змінній result
console.log(result)  */    //Вивід змінної result у консоль



//СТВОРЕННЯ КОПІЇ ОБ'ЄКТА І ЙОГО ВИКОРИСТАННЯ У ФУНКЦІЇ
/*ОПИС: 
Ми створюємо об'єкт personOne.
Далі створюємо функцію increasePersonAge, у якій ми створюємо змінну(об'єкт) updatedPerson, яка буде копією об'єкту personOne для 
використання в середині функції.
Тобто після цього об'єкт updatedPerson буде мати тіж самі властивості як і об'єкт personOne (тобто age і userName).
Після цього у функції ми збільшуємо на 1 властивість age об'єкта updatedPerson. І повертаємо результат за допомогою return.
Далі створюємо нову змінну updatedPersonOne і присвоюємо їй результат виклику функції increasePersonAge, аргументом якого буде об'єкт personOne.
Тобто по факту, ми взяли властивості об'єкта personOne, створили його копію(updatedPerson) всередині функції, збільшили значення властивості
age на одиницю, і результат цих обчислень записали у змінну updatedPersonOne. І вивели цей результат у консоль.
*/
/* const personOne = {     //Створення об'єкта (це наш зовнішній об'єкт з яким ми будемо працювати, але не змінюючи його)
    age: 20,
    userName: 'Hed'
}

function increasePersonAge (person) {
    const updatedPerson = Object.assign({}, person)     //Створення нової змінної(так званої копії) на основі person
    updatedPerson.age += 1      //Тут ми збільшуємо значення age на 1
    return updatedPerson        //І тут повертаємо результат після збільшення age на 1
}

const updatedPersonOne = increasePersonAge(personOne)       //Виклик функції increasePersonAge з аргументом personOne. Цей personeOne підставляється на місце person у функції
console.log(personOne.age)
console.log(updatedPersonOne.age) */



//CALLBACK ФУНКЦІЇ
//Приклад колбек функції:
/* function anotherFunction(){
    //there is some actions
}

function fnWithCallback(callbackFunction){
    callbackFunction()
}

fnWithCallback(anotherFunction) */


/* function printMyName() {
    console.log('HED')
}

setTimeout(printMyName, 1000) */       //Вбудована в JS функція setTimeout робить затримку на 1000мс після чого виконує функцію яку ми створили вище printMyName



//ОБЛАСТІ ВИДИМОСТІ - Вони визначають кордони дії змінних. Є Глобальні і Локальні області. Це можуть бути змінні і тд.
//Приклад:

//Оголошення  Глобальних змінних a та b:

/* let a
let b 

function myFn() {
    let b       //Оголошення Локальної змінної b
    a = true
    b = 10
    console.log(b)      //Тут виведеться число 10, тому що буде викликана Локальна змінна b
}

myFn()

console.log(a)      // Тут буде вивід true
console.log(b)      // Тут буде вивід undefined, тому що буде викликана Глобальна змінна b */



//ЛАНЦЮГ ОБЛАСТІ ВИДИМОСТІ
/* const a = 7   */   //Тут ми об'явили змінну a у Глобальній області видимості

/*Далі ми створюємо функцію myFn і всередині неї створюємо ще одну функцію innerFn у якій ми викликаємо console.log(a)
Що відбувається коли console.log(a) намагається знайти змінну a: він спочатку шукає у області innerFn-не знаходить змінної a. Йде далі і шукає в області myFn-не знаходить.
Йде далі і шукає у Глобальній області, де вже і знаходить змінну a значення якої дорівнює 7.
*/
/* function myFn() {       
    function innerFn() {
        console.log(a)      //Буде виведено число 7.
    }
    innerFn()
}

myFn()
innerFn()   */     //Тут ми отримаємо помилку, тому що такої змінної/функції як innerFn не існує у Глобальній області видимості. Вона існує тільки всередині області видимості myFn



//ОПЕРАТОРИ!!!  Оператор - це по факту вбудована функція в мову програмування
/*
Арифметичні оператори: 
+ Додати 
- Відняти
* Помножити
/ Розділити

Оператори порувняння: 
=== Дорівнює
!== Не дорівнює
<= Більше або дорівнює
>= Менше або дорівнює

Логічні оператори:
! Ні
&& і
||  або (или)

Оператор присвоювання:
= Присвоювання

Текстові оператори:
delete видаляє властивість значення
new 
typeof перевіряє тип значення
instanceof перевіряє принадлежність об'єкта тому чи іншому класу
*/


//ОПЕРАТОР ... - завдяки цьому оператору можна створити новий об'єкт(redButton) на основі іншого(button) . Але значення об'єкту button при цьому НЕ змінюються
/* const button = {
    width: 200,
    text: 'Buy'
}

const redButton = {
    ...button,
    color: 'red'
}

console.table(redButton) //Вивід у вигляді таблиці */


//КОНКАТЕНАЦІЯ РЯДКІВ за допомогою знака +
/* const hello = 'Hello'
const world = 'World'

const greeting = hello + ' ' + world

console.log(greeting) */


//КОНКАТЕНАЦІЯ РЯДКІВ за допомогою ШАБЛОННИХ РЯДКІВ. Шаблонні рядки позначаю наступним чином `${значення/змінна} ${значення/змінна} ${значення/змінна}`
/* const hello = 'Hello'
const world = 'World'

const greeting = `${hello} ${world}`

console.log(greeting) */

//ДОМАШНЄ ЗАВДАННЯ:
/* const myNameIs = 'Мене Звати'
const name = 'Стас'
const iAmFrom = 'Я З Міста'
const city = 'Львів'

const result = `${myNameIs} ${name} ${iAmFrom} ${city}`

console.log(result) */



//ФУНКЦІОНАЛЬНІ ВИРАЗИ
/* const myFunction = function(a, b) {     //function - це і є функціональний вираз. Тобто функція починається з function(a, b) і т.д.. А const myFunction - це змінна
    let c
    a = a + 1
    c = a + b
    return c
}

console.log(myFunction(5, 3)) */


//ФУНКЦІОНАЛЬНИЙ ВИРАЗ У ВИКЛИКУ ІНШОЇ ФУНКЦІЇ

/* setTimeout(function() {
    console.log('Відкладене повідомлення')
}, 1000) */



//СТРІЛОЧНА ФУНКЦІЯ ( не має імені та позначається за рахунок => ) . Також треба розуміти, що стрілочна функція є Виразом

/* (a, b) => {
    let c
    a = a + 1
    c = a + b
    return c
} */

//Приклад, як дати ім'я Стрілочній функції

/* const myFunction = (a, b) => {
    let c
    a = a + 1
    c = a + b
    return c
}

myFunction(5, 3) // 9 */


//Стрілочна Функція у виклику іншої функції
/* setTimeout( () => {
    console.log('MESSAGE!')
}, 1000) */


//Варіанти скорочення Стрілочних функцій:

// #1 - Якщо функція має тільки один параметр "a".
(a) => {
    //Тіло функції
}

// #2 - Якщо тіло функції складається тільки із одного виразу "a + b"
(a, b) => a + b         //У такому випадку функція автоматично вертає результат її роботи.


