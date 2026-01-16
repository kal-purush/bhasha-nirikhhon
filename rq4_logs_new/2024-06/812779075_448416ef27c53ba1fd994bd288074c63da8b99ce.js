precio = 400000


// Seleccion de todos los elementos que interactuan
precioSpan = document.querySelector(".precio-inicial");
cantidadSpan = document.querySelector(".cantidad")
botonIncrementar = document.querySelector(".boton-incrementar")
botonDisminuir = document.querySelector(".boton-disminuir")
valorTotalSpan = document.querySelector(".valor-total")

// Asignacion de valores a las variables
precioSpan.innerHTML = precio
cantidadSpan.innerHTML = 0
valorTotalSpan.innerHTML = 0

// Funcion para incrementar el contador de cantidad al hacer click
botonIncrementar.addEventListener("click", function(){
    cantidad = Number(cantidadSpan.innerHTML); // Al hacer click, crea la variable cantidad, a la cual se le asigna el valor del elemento HTML como tipo Number
    cantidad += 1; // La variable cantidad se incrementa en 1
    cantidadSpan.innerHTML = cantidad; // Se le asigna al elemento HTML el valor de la variable cantidad con el incremento
    actualizarTotal(); // Invoca la funcion actualizar total
})

// Funcion para disminuir el contador de cantidad al hacer click
botonDisminuir.addEventListener("click", function(){
    cantidad = Number(cantidadSpan.innerHTML);
    if (cantidad > 0) { // El if controla que la disminucion de la cantidad no sea menor que 0
        cantidad -= 1;
    }
    cantidadSpan.innerHTML = cantidad;
    actualizarTotal();
})

// Funcion para actualizar el total a pagar
function actualizarTotal(){
    cantidad = Number(cantidadSpan.innerHTML); 
    total = cantidad * precio; 
    valorTotalSpan.innerHTML = total;
}