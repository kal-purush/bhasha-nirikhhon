// El código va aquí -> 
let txtNombre = document.getElementById("Name"); // Nombre
let txtNumber = document.getElementById("Number"); //Cantidad
let btnAgregar = document.getElementById("btnAgregar");
let btnClear = document.getElementById("btnClear");
let alertValidaciones = document.getElementById("alertValidaciones");
let tablaListaCompras = document.getElementById("tablaListaCompras");
let cuerpoTabla = tablaListaCompras.getElementsByTagName("tbody").item(0);
let contadorProductos = document.getElementById("contadorProductos");
let productosTotal = document.getElementById("productosTotal");
let precioTotal = document.getElementById("precioTotal");
let contador = 0;
let precio = 0;
let costoTotal = 0;
let totalEnProductos=0;

let datos = new Array(); // Aquí vamos a almacenar los elementos de la tabla

function validarCantidad(){
    if (txtNumber.value.length==0){
        return false;
    }// if length
    if (isNaN(txtNumber.value)){
        return false;
    }//isNaN
    if(parseFloat(txtNumber.value)<=0){
        return false;
    }//<=0
    if(parseInt(txtNumber.value)!=txtNumber.value){
        return false;
    }//<=0
    return true;
}//validarCantidad

function getPrecio(){
    return Math.floor(((Math.random() *100)*100))/100;
}//getPrecio

btnAgregar.addEventListener("click", function(event){
    let isValid = true;
    event.preventDefault();
    alertValidaciones.innerHTML="";
    alertValidaciones.style.display="none";
    txtNombre.style.border="";
    txtNumber.style.border="";
    if (txtNombre.value.length<3) {
        alertValidaciones.innerHTML="El campo <strong>Nombre</strong> es requerido<br/>";
        alertValidaciones.style.display="block";
        txtNombre.style.border="solid thin red";
        isValid = false;
    }// txtNombre < 3
    if (! validarCantidad()){
        alertValidaciones.innerHTML+="El campo <strong>Cantidad</strong> es requerido<br/>";
        alertValidaciones.style.display="block";
        txtNumber.style.border="solid thin red";
        isValid = false;
    }//if ! validarCantidad
    if (isValid){
        contador++;
        precio = getPrecio();
        let row = `<tr>
                        <td>${contador}</td>
                        <td>${txtNombre.value}</td>
                        <td>${txtNumber.value}</td>
                        <td>${precio}</td>
                    </tr>`;
        let elemento = `{"id" : "${contador}",
                        "nombre": "${txtNombre.value}",
                        "cantidad": "${txtNumber.value}",
                        "precio": "${precio}"}`;
        datos.push( JSON.parse(elemento));
        localStorage.setItem("datos", JSON.stringify(datos));

        cuerpoTabla.insertAdjacentHTML("beforeend", row);
        contadorProductos.innerText= contador;
        totalEnProductos+= Number(txtNumber.value);
        productosTotal.innerText = totalEnProductos;
        costoTotal += (precio * Number(txtNumber.value));
        precioTotal.innerText = `$ ${costoTotal.toFixed(2)}`; 

        localStorage.setItem("contador", contador);
        localStorage.setItem("totalEnProductos", totalEnProductos);
        localStorage.setItem("costoTotal", costoTotal.toFixed(2));


        txtNombre.value="";
        txtNumber.value="";
        txtNombre.focus();
    }//isValid
});// btnAgregar click

btnClear.addEventListener("click", function(event){
    event.preventDefault();
    alertValidaciones.innerHTML="";
    alertValidaciones.style.display="none";
    txtNombre.style.border="";
    txtNumber.style.border="";
    txtNombre.value="";
    txtNumber.value="";
    cuerpoTabla.innerHTML="";
    contador=0;
    contadorProductos.innerText=contador;
    totalEnProductos = 0;
    productosTotal.innerText = totalEnProductos;
    costoTotal = 0;
    precioTotal.innerText = `$ ${costoTotal}`;
    datos = [];
    localStorage.clear();
});// btnClear click

window.addEventListener("load", function(event){
    if (localStorage.getItem("contador")!=null){
        contador = Number(localStorage.getItem("contador"));
    }//contador !=null

    if(localStorage.getItem("totalEnProductos")!=null){
        totalEnProductos= Number(localStorage.getItem("totalEnProductos"));
    }//totalEnProducto!=null

    if(localStorage.getItem("costoTotal")!=null){
        costoTotal= Number(localStorage.getItem("costoTotal"));
    }//costoTotal!=null
    
    if(localStorage.getItem("datos")!=null){
        datos = JSON.parse(localStorage.getItem("datos"));
        datos.forEach((r) => {
            let row = `<tr>
                <td>${r.id}</td>
                <td>${r.nombre}</td>
                <td>${r.cantidad}</td>
                <td>${r.precio}</td>
            </tr>`;
            cuerpoTabla.insertAdjacentHTML("beforeend", row);
        });//forEach
    }//datos!=null
    contadorProductos.innerText = contador;
    productosTotal.innerText = totalEnProductos;
    precioTotal.innerText=  `$ ${costoTotal}`;
});