'use strict';

let useName=prompt('Hello my name is Sara, welcome to my page,what is you name!');
alert('hello '+useName);

let play=prompt('Do you like to play!please answer yes/y or no/n ?');
play=play.toLowerCase();
if (play==='yes'||play==='y'){
  alert ('We will have fun');
  console.log('they like to play so true');
}else if (play==='no'||play=='n'){
    alert ('will take 2 min ,try it');
    console.log('they will try even its false');
}


alert('we will play :How well do you know me!!');

let season=prompt('Do expect that i love summer?please answer yes/y or no/n?')
season=season.toLowerCase();
if (season==='yes'||season==='y'){
    alert('no,I love to play under the rain');
    console.log('your answer if i like summer is false');
}else if (season==='no'||season==='n'){
alert('yes,you are right,winter is my favorite season');
console.log('your answer is true that i dont like summer');
}

let vacation=prompt('Do i love to spend my vacation on the beach?please answer yes/y or no/n?')
vacation=vacation.toLowerCase();
if (vacation==='yes'||vacation==='y'){
    alert('you are absolutely right,i like winter but the beach my love ');
    console.log('your answer if i like beach is true');
}else if (vacation==='no'||vacation==='n'){
alert('you thought i like winter i will not love spend my time next to the beach right !!you are wrong ');
console.log('your answer is false that i dont like beach');
}

let age=prompt('What do you think my age is ? if you think am yonger than 30 type yes/y if not no/n.')
age=age.toLowerCase();
if (age==='yes'||age==='y'){
    alert('you are so sweet , but am 33years old');
    console.log('your answer is yes will be false');
}else if (age==='no'||age==='n'){
alert('yes,you are right,am 33 years old');
console.log('your answer is true that am looking older than 30');
}

let animals=prompt('Do you think i like animals?please answer yes/y or no/n?')
animals=animals.toLowerCase();
if (animals==='yes'||animals==='y'){
    alert('yes , i love animals');
    console.log('your answer if i like animals is true');
}else if (animals==='no'||animals==='n'){
alert('noooo, they are cute');
console.log('your answer is false that i dont like animals');
} 

let cooking=prompt('Do expect that i know how to cook?please answer yes/y or no/n?')
cooking=cooking.toLowerCase();
if (cooking==='yes'||cooking==='y'){
    alert('yes,I like to feed people');
    console.log('your answer is true about that i know how to cook');
}else if (cooking==='no'||cooking==='n'){
alert('why do you think that lol , am a gd cooker');
console.log('your answer is false that i dont know how to cook');
}

alert('Thank you '+useName+' to try our quiz');