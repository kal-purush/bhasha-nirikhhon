// crear una funcion que aplique para todos los botones
// cuando toco '=' guardar el innerHTML de 'res' en el arrayNum
// hacer el split del operation y guardarlo en un array para poder separar el operator del operand1 y operand2
// eval detecta los operadores aritmeticos en string y realiza las operaciones
// to string (2) convierte en binario cualquier numero decimal
let operation = document.getElementById('btnEql');
let operand1 = 0; 
let operand2 = 1;
let operator;
let btn0 = document.getElementById('btn0');
let btn1 = document.getElementById('btn1');
let sum = document.getElementById('btnSum');
let res = document.getElementById('btnSub');
let div = document.getElementById('btnDiv');
let clr = document.getElementById('btnClr');

btn0.onclick = function() {
                document.getElementById('res').innerHTML += operand1;
            };
btn1.onclick = function() {
                document.getElementById('res').innerHTML += operand2;
            };
sum.onclick = function() {
                operator = '+';
                document.getElementById('res').innerHTML += operator;
            };
res.onclick = function() {
                operator = '-';
                document.getElementById('res').innerHTML += operator;
            };
div.onclick = function() {
                operator = '/';
                document.getElementById('res').innerHTML += operator;
            };
clr.onclick = function() {
                document.getElementById('res').innerHTML = '';
            };
operation.onclick = function() {
                let arrayNum = document.getElementById('res').innerHTML;//.split(operator);
                let result = (eval(arrayNum));
                let binaryResult = Math.floor(parseInt(result.toString(2)));
                debugger;
                document.getElementById('res').innerHTML = binaryResult;
            };