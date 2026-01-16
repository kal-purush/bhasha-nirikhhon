var countries=["India","USA","Australia","London"];
//console.log(countries);

var states=new Array("TamilNadu","Kerala","Mumbai","Delhi");//another way to form array.
//console.log(states);
//console.log(states.length);//gives length of array.

states[0]="Hyderabad";//replacing 0th index value.
//console.log(states);

var user=["Angesh","Angesh@gmail.com",2,4,true];
//console.log(user);
user.pop();//removes last element from an array.
user.pop();
//console.log(user);
user.unshift("new value");//adding new element to first index of an array.
//console.log(user);
user.shift();//removing added new element from an array.
//console.log(user);

var colors=["blue","white","black","purple"];
// console.log(colors.indexOf("purple"));//returns the index value of element.
// console.log(colors.indexOf("orange"));//returns -1 when element is absent.

console.log(Array.from("Smart"));//convert string into array.