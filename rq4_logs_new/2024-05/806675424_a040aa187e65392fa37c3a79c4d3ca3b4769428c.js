// javascript for output

let in = document.getElementById('input');
let out = document.getElementById('output');

switch (in.textContent) {
case 'option one':
out.textContent = "you selected option one.";
break;
default:
out.textContent = "select an option.";
}