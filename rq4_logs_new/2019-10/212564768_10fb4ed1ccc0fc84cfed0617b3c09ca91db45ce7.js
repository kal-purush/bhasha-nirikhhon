/*
Escribe una función que acepte un número como argumento. La función retornará el número opuesto. 
Ejemplo:
miFuncion(40) → -40
miFuncion(-70) → 70
*/


function  negativarNumero() {
    let valor = prompt("Numero:");
    document.write(valor * (-1));
    return true; 
}



let pulsador = document.querySelectorAll("body > a")[0];

pulsador.addEventListener("click", negativarNumero);
