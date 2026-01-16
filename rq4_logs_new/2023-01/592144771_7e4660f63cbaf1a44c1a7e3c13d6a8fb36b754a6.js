//CLASES
class Juego {
  constructor(nombre, precio) {
    this.nombre = nombre;
    this.precio = precio;
  }
}

const gow = new Juego("God Of War: Ragnarok", 10000);
const horizon = new Juego("Horizon Forbidden West", 8000);
const elden = new Juego("Elden Ring", 12000);
const fifa = new Juego("Fifa 23", 9000);
const spiderman = new Juego("Spider-Man", 7000);
const juegos = [gow, horizon, elden, fifa, spiderman];

class Usuario {
  constructor(nombre, saldo) {
    this.nombre = nombre;
    this.saldo = saldo;
  }
}

const usuario1 = new Usuario("Felipe", 30000);
const usuario2 = new Usuario("Julio", 15000);
const usuario3 = new Usuario("Emilio", 9000);
const usuario4 = new Usuario("Rogelio", 100000);

let usuarios = [
  usuario1.nombre,
  usuario2.nombre,
  usuario3.nombre,
  usuario4.nombre,
];

//Menu Juegos

let juegoIngresado = "";
numero = 1;
function menuJuegos() {
  let bucle = false;

  while (!bucle) {
    do {
      numero = 1;
      console.clear();
      console.log(`Que juego desea comprar?`);
      for (const lista of juegos) {
        console.log(numero + ". " + lista.nombre + " $" + lista.precio);
        numero++;
      }
      console.log(`6. Retroceder`);
      juegoIngresado = parseInt(prompt(`Ingrese la opcion`));
      if (juegoIngresado < 1 || juegoIngresado > 6 || isNaN(juegoIngresado)) {
        alert("Opción no válida");
      }
    } while (
      !(juegoIngresado > 0 && juegoIngresado < 7 && !isNaN(juegoIngresado))
    );
    switch (juegoIngresado) {
      case 1:
        alert(`Añadido al carrito de compras`);
        añadirJuego(gow);
        break;
      case 2:
        alert(`Añadido al carrito de compras`);
        añadirJuego(horizon);
        break;
      case 3:
        alert(`Añadido al carrito de compras`);
        añadirJuego(elden);
        break;
      case 4:
        alert(`Añadido al carrito de compras`);
        añadirJuego(fifa);
        break;
      case 5:
        alert(`Añadido al carrito de compras`);
        añadirJuego(spiderman);
        break;
      case 6:
        bucle = true;
        console.clear();
        break;
    }
  }
}

//Añadir juego
let carrito = [];
let carritoTotal = 0;
function añadirJuego(juego) {
  carrito.push(juego.precio);
  carritoTotal = carrito.reduce((acc, juego) => acc + juego, 0);
}

//Finalizar compra
let ingresarUsuario;
function finalizarCompra() {
  if (confirm("Desea finalizar la compra?")) {
    ingresarUsuario = prompt("Ingrese su nombre de usuario");
    while (ingresarUsuario !== usuarios.nombre) {
      ingresarUsuario = prompt("Ingrese su nombre de usuario");
    }
  }
}


//Ver carrito
function menuCarrito() {
      console.log(`Qué desea hacer?
        1. Ver lista de compras
        2. Realizar pago
        3. Retroceder`);
      console.clear();
let dato = 0;
let bucle = false;

  while (!bucle) {
    do {
      console.log(`Qué desea hacer?
      1. Ver Total
      2. Realizar pago
      3. Retroceder`);
      dato = parseInt(prompt("Digite la opción: "));
      console.clear();
      if (dato < 1 || dato > 3 || isNaN(dato)) {
        alert("Opción no válida");
        console.clear();
      }
    } while (!(dato >= 1 && dato <= 3));
    switch (dato) {
      case 1:
        alert(`El total de compras es: ${carritoTotal}`);
        break;
      case 2:
        alert("El pago se ha realizado con éxito")
        ;
        break;
      case 3:
        bucle = true;
        break;
    }
  }
    }

//Menu opciones

function menuOpciones() {
  let dato = 0;
  let bucle = false;

  while (!bucle) {
    do {
      console.log(`Qué desea hacer?
            1. Comprar juegos
            2. Ver carrito
            3. Retroceder`);
      dato = parseInt(prompt("Digite la opción: "));
      console.clear();
      if (dato < 1 || dato > 3 || isNaN(dato)) {
        alert("Opción no válida");
        console.clear();
      }
    } while (!(dato >= 1 && dato <= 3));
    switch (dato) {
      case 1:
        menuJuegos();
        break;
      case 2:
        menuCarrito();
        break;
      case 3:
        bucle = true;
        break;
    }
  }
}

//Iniciar sesion
function iniciarSesion() {
  let bucle = false;
  let usuario = "";
  let respuesta = false;

  while (!bucle) {
    console.log("INICIO DE SESION\n");
    usuario = prompt("Digite su nombre de usuario: ");
    if (usuarios.includes(usuario)) {
      bucle = true;
      alert(`Bienvenido ${usuario}!`);
      respuesta = true;
    }
    console.clear();
  }
  return respuesta;
}

//PRINCIPAL

function main() {
  let bucle = false;

  while (!bucle) {
    do {
      console.log(`Qué desea hacer?
                  1. Iniciar sesion
                  2. Salir`);
      dato = parseInt(prompt("Digite la opción: "));
      console.clear();
      if (dato < 1 || dato > 2 || isNaN(dato)) {
        alert("Opción no válida");
        console.clear();
      }
    } while (!(dato >= 1 && dato <= 2));
    switch (dato) {
      case 1:
        if (iniciarSesion()) {
          menuOpciones();
        }
        break;
      case 2:
        alert("Hasta Luego!");
        bucle = true;
        break;
    }
  }
}

main();