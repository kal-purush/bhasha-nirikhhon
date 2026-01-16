    /* 1
    document.writeln("Hola mundo!");
	document.writeln("Bienvenidos!");  
	/*Writeln Muestra el mensaje como write, pero a diferencia 
	que writeln agrega la nueva linea al final*/




/* 2
let a, b;
a = 10;
b = 5;
resultado = a + b;

alert ('La suma de a+b es' + ': ' + resultado);

/* "let" es para invocar la variable.
"a, b y resultado" son los nombres de las variables.
y "alert" es para mostrar una ventana emergente, mostrando 
la concatenación*/





 /*3
let num1 = 0;
let num2 = 0;

num1 = num1 + 11;
num2 = num2 + 5;
/*"let" es para invocar la variable.
"num1 y num2" son los nombres de las variables.*/

/*
alert('Ahora el numero 1 es ' + num1);
alert('Ahora el numero 2 es ' + num2);*/

 /*y "alert" es para mostrar una ventana emergente, mostrando 
la concatenación que dice que el numero 1 aunque su valor sea 0,
al llamar la variable se sumara 1 y en el caso de numero 2 sumara 5 */






 /*4
const a = "Hola ";
let b = "mundo!";

document.write('constante a contiene a: ' + a);
document.write("</br>");
document.write('variable b contiene a: ' + b);
document.write("</br>");
document.write(a + b);*/
/*Const es la constante "a" y let es la variable "b", 
lo que ejecuta es que "a" es = Hola y "b" es = a Mundo!*/




/*5
let a, b;
a = 2; b = 8;
resultado = a * b;

document.write('variable a contiene ' + a );
document.write("</br>");
document.write('variable b contiene ' + b );
document.write("</br>");
document.write('El producto de a por b es ' + resultado);
*/

/* let es una variable de dos datos "a" y "b" y como
 resultado se multiplicaran los dos datos "a" y "b"*/



/* 6
let dato, resultado;

dato = window.prompt("Introduce tu nombre", "0");
resultado = 'Hola, como estas ' + dato;

document.write(resultado);*/

/* let es una variable de dos datos "dato" y "resultado", window.prompt es para introducir datos
 en este caso un nombre y ejecuta resultado*/




/* 7

let dato, num; 
dato = window.prompt("Introduce numero ?", "0");
num = parseInt(dato);

 num = num *2;

document.write('El doble es' + num);*/

/* let llama a dos variables "dato" y "num", dato contiene un prompt para ingresar un numero
 y num requiere que sea un dato de tipo entero y que este sea mutiplicado por 2, ejecutando num *2*/





/*8
let dato1, dato2, num1, num2;

dato1 = window.prompt("Introduce primer numero", "0");
num1 = parseInt(dato1);
dato2 = window.prompt("Introduce segundo numero", "0");
num2 = parseInt(dato2);

let resultado = num1 + num2;


document.write('La suma es ' + resultado);
*/

/* cuatro variables "dato1", "num1", "dato2", "num2", dato1 y dato 2 
contienen un prompt para introducir el primer y segundo numero y num1 y num2, son los numeros ingresados
el resultado es la suma de num1 y num2.
*/
   




/* 9 
let dato, num;

dato = window.prompt("Introduce un numero", "0");
num = parseInt(dato);

let resultado = num *2;

document.getElementById("salida").innerHTML = ('El doble es' + resultado);
*/

/*  dos variables "dato" y "num" dato contiene un prompt para ingresar un 
numero de tipo entero "parseInt"
este sera multiplicado por 2, dando como resultado el doble del numero.
*/





10
var contador;
contador = 1;


while (contador < 10)
{
	let dato = prompt('Introduce numero del 1 al 10: ', '');
	let num = parseInt(dato);

	document.write('El numero introducido es: ' + num);
	document.write('<br>');
	document.write('El contador es: ' + contador);
	document.write('<br>');

	contador = contador + 1;
}
document.write('<br>');
document.write('Fin del programa el contador ya NO es menor que 10.');
document.write('<br>');
document.write('Ultimo numero introducido es' + num)


/* la variable contador equivale a 1, y mientras contador sea menor a 5
la variable dato, es para introducir 5 numeros del 1 al 10, se ejecuta 
"el numero introducido es + el numero que introduce" y sumara 1 al contador, cuando llega a 5
el contador ejecuta "Fin de programa"*/