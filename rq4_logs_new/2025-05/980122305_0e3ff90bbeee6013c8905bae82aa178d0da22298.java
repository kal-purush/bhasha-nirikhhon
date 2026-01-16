const productos = [
  { id: 1, nombre: "Camiseta Biodegradable", precio: 35.0, categoria: "camiseta", img: "https://ciclico.com.co/cdn/shop/files/Camiseta_gris_conexiones_espalda1.webp?v=1742312327&width=600" },
  { id: 2, nombre: "Pantalón Sostenible", precio: 50.0, categoria: "pantalón", img: "https://sientochenta.com/cdn/shop/files/pantalon-Olu.png?v=1697519247&width=600" },
  { id: 3, nombre: "Zapatos Ecológicos", precio: 80.0, categoria: "zapatos", img: "https://www.clotsybrand.com/cdn/shop/files/desglose-caracteristicas.webp?v=1741188399&width=1080" },
  { id: 4, nombre: "Gorra Reciclada", precio: 20.0, categoria: "gorra", img: "https://www.kupa.co/cdn/shop/files/cholon-pesquero-gorro-estampado-manchas-granate-rojo-rosado-kupa.jpg?v=1709741952&width=1440" },
  { id: 5, nombre: "Mochila Biodegradable", precio: 60.0, categoria: "mochila", img: "https://www.kupa.co/cdn/shop/files/MiniCocuy-dinosaurios-manchas-verdes-dino--maleta-nino-nina-adulto-colegio-colores-tierra-anyurasi-park.jpg?v=1738676692&width=1440" },
  { id: 6, nombre: "Sumapaz Chaqueta", precio: 239.9, categoria: "chaqueta", img: "https://www.kupa.co/cdn/shop/products/Sumapaz_Prisma-10.jpg?v=1678825139&width=1440" },
  { id: 7, nombre: "Chaqueta Acolchada Sumapaz", precio: 359.9, categoria: "chaqueta", img: "https://www.kupa.co/cdn/shop/files/PiensoEnTi-17.jpg?v=1738676393&width=1440" }
];

let carrito = [];

const contenedorProductos = document.getElementById("productos");
const listaCarrito = document.getElementById("lista-carrito");
const totalCarrito = document.getElementById("total");

function mostrarProductos(lista = productos) {
  contenedorProductos.innerHTML = "";
  lista.forEach(prod => {
    const div = document.createElement("div");
    div.className = "producto";
    div.onclick = () => div.classList.toggle("selected");
    div.innerHTML = `
      <img src="${prod.img}" alt="${prod.nombre}">
      <h3>${prod.nombre}</h3>
      <p>Precio: $${prod.precio.toFixed(2)}</p>
      <button onclick="agregarAlCarrito(${prod.id}); event.stopPropagation();">Agregar</button>
      <button onclick="quitarDelCarrito(${prod.id}); event.stopPropagation();">❌</button>
    `;
    contenedorProductos.appendChild(div);
  });
}

function filtrarProductos() {
  const categoria = document.getElementById("filtro-categoria").value;
  if (categoria === "todos") mostrarProductos();
  else mostrarProductos(productos.filter(p => p.categoria === categoria));
}

function agregarAlCarrito(id) {
  const prod = productos.find(p => p.id === id);
  carrito.push(prod);
  actualizarCarrito();
}

function quitarDelCarrito(id) {
  carrito = carrito.filter(item => item.id !== id);
  actualizarCarrito();
}

function actualizarCarrito() {
  listaCarrito.innerHTML = "";
  let total = 0;
  carrito.forEach(item => {
    const li = document.createElement("li");
    li.textContent = `${item.nombre} - $${item.precio.toFixed(2)}`;
    listaCarrito.appendChild(li);
    total += item.precio;
  });
  totalCarrito.textContent = total.toFixed(2);
}

function vaciarCarrito() {
  carrito = [];
  actualizarCarrito();
}

function procesarPago(event) {
  event.preventDefault();
  alert("Datos enviados. Procesaremos tu pago pronto.");
  vaciarCarrito();
  document.getElementById("formulario-pago").reset();
}

// PayPal
if (window.paypal) {
  paypal.Buttons({
    createOrder: (data, actions) => actions.order.create({
      purchase_units: [{ amount: { value: totalCarrito.textContent } }]
    }),
    onApprove: (data, actions) => actions.order.capture().then(details => {
      alert(`Pago realizado por ${details.payer.name.given_name}`);
      vaciarCarrito();
    })
  }).render('#paypal-button-container');
}

// Inicializar
mostrarProductos();