"use strict";

var nombre = "Daan Valencia";
var edad = 20;
console.log("Hello world"); //var alerta = alert("Alerta: que no se te olviden los \' simples \' \n pero tambien puedes usar \"Dobles\"");

document.write("Una forma de imprimir");
var imprimir = document.getElementById("print");
imprimir.innerHTML = "What's up!";
imprimir.innerHTML = "\n    <h2>Hola soy ".concat(nombre, " </h2>\n    <h2>Y tengo ").concat(edad, " </h2>\n");
/* function no necesita retornar pero lo hace y 
retorna lo que sea que uno quiera sin ser declarado, 
el innerHTML*/