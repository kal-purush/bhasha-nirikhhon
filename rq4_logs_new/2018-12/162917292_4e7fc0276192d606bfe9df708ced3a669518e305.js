var age = 19;

var job = 'instructor';
switch(true)
{
    case age < 18:
    case age === 18:
        console.log('<=18');
    break;
    default:
        console.log('>18');
    break;
    
}


/*
var job = 'instructor';
switch(job)
{
    case 'teacher':
    case 'instructor':
        console.log('teaches');
    break;
    case 'driver':
        console.log('drives');
    break;
    default:
        console.log('someething else');
    break;
    
}


var firstName = 'John';
var civilStatus = 'married';
var age = 16;

var drink  = age >= 18 ? 'beer' : 'juice';
console.log(firstName + ' drinks ' + drink);


if(civilStatus === 'married')
{
    console.log(firstName + ' is married!');
}
else if (civilStatus === 'single' && age < 18 )
{
    console.log(firstName + ' too young for marriage');
}
else
{
    console.log(firstName + ' single!');
}




var mMark = 78;
var hMark = 1.69;

var mJohn = 92;
var hJohn = 1.95;

var bmiMark = mMark / (hMark **2);
var bmiJohn = mJohn / (hJohn **2);

var bmiHigher = bmiMark > bmiJohn;

console.log('bmiMark = ' + bmiMark + ' is higher than bmiJohn =' + bmiJohn + ': ' + bmiHigher);
*/


/*
//Logical operators
var currentYear = 2018
var yearJohn = 1989;
var ageJohn = currentYear - yearJohn;
var fullAge = 18;

var isFullAge = currentYear - yearJohn >= fullAge;

console.log(isFullAge);

//Math
var birthYearJohn = currentYear - ageJohn;
var birthYearMark = currentYear - ageMark;

//typeof operator
console.log(typeof (ageJohn < ageMark));
console.log(typeof ageJohn);
var x;
console.log(typeof x);


var firstName = "John";
var lastName = "Smith";
var age = 20;
var fullAge = true;
console.log(lastName);
console.log(fullAge);

var isMarried = false;

console.log(firstName + ' is ' + age + ' years old. Married: ' + isMarried);

//Var mutation
age = 'twenty';

//alert(firstName + ' is ' + age + ' years old. Married: ' + isMarried);

var firstName = prompt('Last name? ');
console.log(firstName + ' is ' + age + ' years old. Married: ' + isMarried);
*/