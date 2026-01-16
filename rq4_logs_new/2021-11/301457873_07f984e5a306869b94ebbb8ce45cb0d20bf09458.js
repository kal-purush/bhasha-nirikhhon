//creamos la variable formulario para tener los campos dentro de ella y poder utilizarlo
let formulario = {
    nombre: document.getElementById("nombre"),
    telefono: document.getElementById("telefono")
}
//creamos la variable agenda que sera lo que usemos para manejar el almacenamiento local
let agenda;
//ya que vamos a usar posicion: relative creamos dos variables que nos permitira tener la posicion inicial del objeto a mover
let antiguaX, antiguaY;
//la variable seleccion nos marcara el objeto seleccionado
let seleccion = "";
//usaremos una variable para el intervalo que asignaremos cuando sea necesario
let intervalo;

//esta funcion eliminara un registro del array en la memoria local
function eliminar() {
    //solo se ejecutara si hay alguna fila seleccionada
    if (seleccion.id === "fila") {
        //paramos el evento que nos captura el movimiento del raton
        document.removeEventListener("mousemove", movimiento);
        //como capturamos toda la columna cogeremos el texto que tenga y lo separaremos con un split para tener solo el nombre
        let nombre = seleccion.innerText;
        nombre = nombre.split(/\s/)
        //creamos un array probisional que nos guardara todo lo que no sea ese nombre
        let arrayProvisional;
        arrayProvisional = agenda.filter((dato) => {
            return dato.nombre != nombre[0];
        })
        //sobreescribimos el array agenda y lo guardamos en la memoria
        agenda = arrayProvisional;
        localStorage.setItem("agenda", JSON.stringify(agenda));
        //asignamos el intervalo para que haga una especie de animacion que lance el objeto hacia arriba para que se elimine
        intervalo = setInterval(borrarLanzando, 10)

    }
}
//la funcion borrarLanzando hara ascender la columna seleccionada hasta que desaparezca
function borrarLanzando() {
    //como tiene posicion relativa en vez de usar el offset cogeremos directamente la posicion en style y lo separamos de los px para obtener solo el numero entero
    let pos = seleccion.style.top.split(/[p]/)
    //y lo vamos moviendo en funcion de eso
    seleccion.style.top = `${pos[0] - 2}px`;
    //en caso de que toque la parte superior de la pantalla hacemos desaparecer el objeto y limpiamos el intervalo
    if (seleccion.offsetTop <= 0) {
        seleccion.style.display = "none"
        clearInterval(intervalo)
    }
}
//la funcion mover seleccionara la columna que queramos desplazar a la papelera
function mover(evento) {
    //como hemos usado delegacion de eventos usaremos la target.parentElement para obtener en vez de la celda la fila completa
    seleccion = evento.target.parentElement;
    //para que vaya junto al raton almacenaremos la posicion antigua del objeto porque lo tenemos en posicion relativa
    antiguaY = seleccion.offsetTop
    antiguaX = seleccion.offsetLeft
    //añadimos un evento de lectura del movimiento del raton
    document.addEventListener("mousemove", movimiento);
}
function movimiento(evento) {
    //si tenemos seleccionada una fila moveremos el objeto seleccionado en funcion de la posicion del raton menos la posicion inicial
    if (seleccion.id == "fila") {
        seleccion.style.top = `${evento.clientY - antiguaY}px`;
        seleccion.style.left = `${evento.clientX - antiguaX}px`;
    }
}
//la funcion guardar introducira en la memoria el array agenda
function guardar() {
    //primero capturamos lo que haya en la memoria
    agenda = JSON.parse(localStorage.getItem("agenda"));
    //si no hay nada hacemos que agenda sea un array
    if (agenda == null) {
        agenda = [];
    }
    //creamos el contacto
    let contacto = {
        nombre: formulario.nombre.value,
        telefono: formulario.telefono.value
    }
    //lo añadimos al array y lo guardamos
    agenda.push(contacto);
    localStorage.setItem("agenda", JSON.stringify(agenda))
    //reiniciamos el formulario
    formulario.nombre.value = "";
    formulario.telefono.value = "";

}
//la funcion mostrar agenda nos creara una tabla con los datos que tenga la memoria
function mostrarAgenda() {
    let cadenaTexto = "";
    agenda = JSON.parse(localStorage.getItem("agenda"));
    if (agenda == null)
        cadenaTexto = "La agenda esta vacia"
    else {
        cadenaTexto = "<table id=\"mostrado\"><tr><th>Nombre</th><th>telefono</th></tr>"
        agenda.forEach(contacto => {
            cadenaTexto += `<tr id="fila"><td>${contacto.nombre}</td><td>${contacto.telefono}</td></tr>`
        });
        cadenaTexto += "</table>";
    }
    //lo mostraremos en el div con id mostrarTexto
    mostrarTexto.innerHTML = cadenaTexto;
    //crearemos las escuchas de eventos correspondientes en la tabla creada(id=mostrado) y en la tabla id=papeleraTabla
    mostrado.addEventListener("mousedown", mover);    
    papeleraTabla.addEventListener("mousedown", eliminar);
}

//capturamos los botones con un evento onclick de dos maneras la primera usando get element by id
//y la segunda utilizando directamente la Id que lo permite JS
document.getElementById("agregar").onclick = guardar;
mostrar.onclick = mostrarAgenda;