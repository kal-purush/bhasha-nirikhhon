/* 
console.table(productos);
const carrito = [];
let contenedor = document.getElementById("misprods");

function renderizarProductos(){
    for(const producto of productos){
        contenedor.innerHTML += `
            <div class="card col-sm-2">
                <img src=${producto.img} class="card-img-top" alt="...">
                <div class="card-body">
                    <h5 class="card-title">${producto.id}</h5>
                    <p class="card-text">${producto.nombre}</p>
                    <p class="card-text">$ ${producto.precio}</p>
                    <button id='btn${producto.id}' class="btn btn-primary align-bottom">Comprar</button>
                </div>
            </div>   
        `;
    }
    
    //EVENTOS
    productos.forEach((producto)=>{
        document.getElementById(`btn${producto.id}`).addEventListener('click',()=>{
            agregarACarrito(producto);
    });
});
}

renderizarProductos();

function agregarACarrito(prodAAgregar){
    carrito.push(prodAAgregar);
    console.table(carrito);
    alert(`Agregaste ${prodAAgregar.nombre} al carrito ! 💪`);
    //agregar fila a la tabla de carrito
    document.getElementById('tablabody').innerHTML += `
        <tr>
            <td>${prodAAgregar.id}</td>
            <td>${prodAAgregar.nombre}</td>
            <td>${prodAAgregar.precio}</td>
        </tr>
    `;
    //incrementar el total
    let totalCarrito = carrito.reduce((acumulador,producto)=>acumulador+producto.precio,0);
    document.getElementById('total').innerText = 'Total a pagar $: '+totalCarrito;
} */

console.table(productos);
let carrito = [];
let contenedor = document.getElementById("misprods");

function renderizarProductos(){
    for(const producto of productos){
        contenedor.innerHTML += `
            <div class="card col-sm-2">
                <img src=${producto.img} class="card-img-top" alt="...">
                <div class="card-body">
                    <h5 class="card-title">${producto.id}</h5>
                    <p class="card-text">${producto.nombre}</p>
                    <p class="card-text">$ ${producto.precio}</p>
                    <button id='btn${producto.id}' class="btn btn-primary align-bottom">Comprar</button>
                </div>
            </div>   
        `;
    }
    
    //EVENTOS
    productos.forEach((producto)=>{
        document.getElementById(`btn${producto.id}`).addEventListener('click',()=>{
            agregarACarrito(producto);
        });
    });
}

// Función para agregar un producto al carrito
function agregarACarrito(prodAAgregar){
    carrito.push(prodAAgregar);
    console.table(carrito);
    alert(`Agregaste ${prodAAgregar.nombre} al carrito ! 💪`);
    
    // Agregar fila a la tabla de carrito
    document.getElementById('tablabody').innerHTML += `
        <tr>
            <td>${prodAAgregar.id}</td>
            <td>${prodAAgregar.nombre}</td>
            <td>${prodAAgregar.precio}</td>
        </tr>
    `;
    
    // Incrementar el total
    let totalCarrito = carrito.reduce((acumulador,producto)=>acumulador+producto.precio,0);
    document.getElementById('total').innerText = 'Total a pagar $: '+totalCarrito;
    
    // Guardar carrito en local storage
    localStorage.setItem('carrito', JSON.stringify(carrito));
}

// Obtener el carrito guardado en local storage al cargar la página
window.onload = function(){
    const carritoGuardado = localStorage.getItem('carrito');
    if(carritoGuardado){
        carrito = JSON.parse(carritoGuardado);
        console.table(carrito);
        
        // Renderizar la tabla de carrito con los productos guardados
        const tablaBody = document.getElementById('tablabody');
        let tablaHTML = '';
        let totalCarrito = 0;
        for(const producto of carrito){
            tablaHTML += `
                <tr>
                    <td>${producto.id}</td>
                    <td>${producto.nombre}</td>
                    <td>${producto.precio}</td>
                </tr>
            `;
            totalCarrito += producto.precio;
        }
        tablaBody.innerHTML = tablaHTML;
        document.getElementById('total').innerText = `Total a pagar $: ${totalCarrito}`;
    }
}

renderizarProductos();