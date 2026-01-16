precio = 400000

precioSpan = document.querySelector(".precio-inicial");
precioSpan.innerHTML = precio;

cantidad = 0
cantidadSpan = document.querySelector(".cantidad");
cantidadSpan.innerHTML = cantidad;

total = precio * cantidad;
totalSpan = document.querySelector(".valor-total");
totalSpan.innerHTML = total

function moreItems(){
    document.getElementsByClassName("cantidad");
    cantidad ++;
    cantidadSpan.innerHTML = cantidad;

    total = precio * cantidad;
    totalSpan = document.querySelector(".valor-total");
    totalSpan.innerHTML = total
}

function lessItems(){
    document.getElementsByClassName("cantidad");
     if (cantidad > 0) {
    cantidad --;
    cantidadSpan.innerHTML = cantidad;}

    total = precio * cantidad;
    totalSpan = document.querySelector(".valor-total");
    totalSpan.innerHTML = total
}

