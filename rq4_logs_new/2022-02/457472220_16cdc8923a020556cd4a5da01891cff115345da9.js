// Import stylesheets
import './style.css';

// Write Javascript code!
const appDiv = document.getElementById('app');
appDiv.innerHTML = `<h1>JS Starter</h1>`;

// 1 Constructor Pattern

let instance01 = {};
let instance02 = Object.create(Object.prototype);
let instance03 = new Object();

console.log('instance01', instance01);
console.log('instance02', instance02);
console.log('instance03', instance03);