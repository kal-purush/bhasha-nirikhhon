'use strict';

var allStudents = new XMLHTTPREQUEST();
allStudents.open('GET', file, false);

console.log('allStudents :', allStudents);
console.log('allStudents property: ' + allStudents.abort);
console.log(allStudents.length);

var main = document.getElementById('main');
console.log(main);