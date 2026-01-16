var a = Number(prompt('Веддіть змінну А'));
var b = Number(prompt('Веддіть змінну B'));
var c = Number(prompt('Веддіть змінну C'));

var result = qudraticEquation(a, b, c);
document.write(result);
function qudraticEquation(a, b, c){
    
    var result;
    if (a === 0){
        result = 'Рівнняння не квадратне, введіть А відмінне від нуля.'
    if (d === 0) {
       result = 'Рівняння має один корінь'
   if (d > 0) {result = 'рівняння має два корені'
    }
       }
}

var d = calcD(a, b, c);

  var x1 = (-b - Math.sqrt(d))/(2*a);
  var x2 = (-b + Math.sqrt(d))/(2*a);
  
  return 'x1 = ' + x1 + ', x2 = ' + x2;

function calcD (a, b, c) {
return b*b - 4*a*c; 
}
}