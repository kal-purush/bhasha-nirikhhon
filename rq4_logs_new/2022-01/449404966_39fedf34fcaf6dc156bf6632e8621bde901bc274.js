// Task 1

let a = 3;
let b = 5;
let c = undefined;
let output = '';

output += 'let a = 3\n';
output += 'let b = 5\n';
output += 'let c = undefined\n';
output += '--------------------------------------------------------\n'
output += 'a + b = ' + (a + b) + '\n';
output += 'a - b = ' + (a - b) + '\n';
output += 'a * b = ' + (a * b) + '\n';
output += 'a / b = ' + (a / b) + '\n';
output += 'a % b = ' + (a % b) + '\n';
output += 'a += b = ' + (a += b) + '\n';
output += 'a -= b = ' + (a -= b) + '\n';
output += 'a *= b = ' + (a *= b) + '\n';
output += 'a /= b = ' + (a /= b) + '\n';
output += 'a %= b = ' + (a %= b) + '\n';
output += 'a == b = ' + (a == b) + '\n';
output += 'a != b = ' + (a != b) + '\n';
output += 'a > b = ' + (a > b) + '\n';
output += 'a < b = ' + (a < b) + '\n';
output += '!a && !c  = ' + (!a && !c ) + '\n';
output += '!a || !c  = ' + (!a || !c ) + '\n';

alert(output)

// Task 2

let first_name = "Yaroslav";
let last_name = "Mishchenko";
let email = "mish0017@algonquinlive.com"
let a2_output;

a2_output = `My name is ${first_name} ${last_name}. You can contact me at ${email}.`;

alert (a2_output);