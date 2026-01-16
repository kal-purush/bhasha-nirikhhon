document.addEventListener("DOMContentLoaded", function () {
  // Galería infinita
  const galeriaInner = document.getElementById("galeriaInner");
  if (galeriaInner) {
    const imagenes = galeriaInner.innerHTML;
    galeriaInner.innerHTML += imagenes;
  }

  // Popup y formulario
  const botonVotar = document.querySelector(".boton-votar");
  const popup = document.getElementById("popup");
  const cerrarPopup = document.getElementById("cerrar-popup");
  const overlay = document.querySelector(".popup-overlay");

  botonVotar.addEventListener('click', function () {
    popup.classList.remove('oculto');
    document.body.classList.add('body-sin-scroll');
  });

  cerrarPopup.addEventListener('click', cerrar);
  overlay.addEventListener('click', cerrar);

  function cerrar() {
    popup.classList.add('oculto');
    document.body.classList.remove('body-sin-scroll');
  }

  // Manejo del envío del formulario
  document.getElementById("formulario").addEventListener("submit", function(event) {
    event.preventDefault();

    const valor_nombre = document.getElementById("nombre").value.trim();
    const valor_correo = document.getElementById("correo").value.trim();
    const valor_edad = document.getElementById("edad").value.trim();

    // Mostrar feedback visual en la página
    document.getElementById("nombre-placeholder").innerText = valor_nombre;
    document.getElementById("correo-placeholder").innerText = valor_correo;
    document.getElementById("feedback").classList.remove("oculto");
    document.getElementById("formulario").classList.add("oculto");

    // Ocultás el contenedor que tiene el título y el form
  document.getElementById('formulario-container').classList.add('oculto');

    // Mostrar feedback en consola con los datos ingresados
    console.log(`Datos recibidos — Nombre: ${valor_nombre}, Correo: ${valor_correo}, Edad: ${valor_edad}`);
  });
});

//QUIENES SONNNNNNN CARDS
const cards = document.querySelectorAll('.card-q');
const leftArrow = document.querySelector('.left-arrow');
const rightArrow = document.querySelector('.right-arrow');

let currentIndex = 0;

function showCard(index) {
  cards.forEach((card, i) => {
    card.classList.toggle('active', i === index);
  });
}

leftArrow.addEventListener('click', () => {
  currentIndex = (currentIndex - 1 + cards.length) % cards.length;
  showCard(currentIndex);
});

rightArrow.addEventListener('click', () => {
  currentIndex = (currentIndex + 1) % cards.length;
  showCard(currentIndex);
});

// Mostrar la primera card al cargar
showCard(currentIndex);
