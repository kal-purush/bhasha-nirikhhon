"use strict";

function isNumberInRange(number){
    if(0 < number && number < 10){
        return console.log('true');
    }
    else{
    return console.log('false');
}
}
isNumberInRange(15);

function isEven(number){
    if(number % 2 == 0){
        return console.log('true');
    }
    else{
        return console.log('false');
    }
}
isEven(111);

let value = +prompt('value?', '');
switch(value){
    case 0:
        alert(0);
        break;
    case 1:
        alert(1);
        break;
    case 2:
    case 2:
        alert('2,3')
        break;
}

function min(a,b){
    if( a > b ){
    return console.log(b);
}
    return console.log(a);
}
    min(3, 5);
    min(5, -1);