import { consumir_funcion } from "../supabase/operaciones.js";

// Selecciona los elementos del DOM
const p_nombre = document.querySelector("#p_nombre");
const p_apellido = document.querySelector("#p_apellido");
const p_direccion = document.querySelector("#p_direccion");
const p_telefono = document.querySelector("#p_telefono");
const p_email = document.querySelector("#p_email");
const p_comentarios = document.querySelector("#p_comentarios");
const form = document.querySelector("#formulario-contacto");

// Obtiene la fecha actual en formato YYYY-MM-DD
function obtenerFechaActual() {
  const fecha = new Date();
  const año = fecha.getFullYear();
  const mes = String(fecha.getMonth() + 1).padStart(2, '0'); // Mes en formato MM
  const dia = String(fecha.getDate()).padStart(2, '0');       // Día en formato DD
  return `${año}-${mes}-${dia}`;
}

// Añade el evento de envío al formulario
form.addEventListener("submit", async (e) => {
  e.preventDefault();

  // Crea un objeto de datos con los valores actuales del formulario
  const dataContacto = {
    p_nombres: p_nombre.value, // Asegúrate de que estos nombres coincidan
    p_apellidos: p_apellido.value,
    p_correo: p_email.value,
    p_telefono: p_telefono.value,
    p_direccion: p_direccion.value,
    p_fecha_registro: obtenerFechaActual(),
    p_fecha_envio: obtenerFechaActual(),
    p_estado: "inicio",
    p_comentarios: p_comentarios.value,
  };

  // Verifica si hay campos vacíos
  if (
    dataContacto.p_nombres === "" ||
    dataContacto.p_apellidos === "" ||
    dataContacto.p_telefono === "" ||
    dataContacto.p_correo === "" || 
    dataContacto.p_comentarios === "" ||
    dataContacto.p_direccion === ""
  ) {
    alert("Todos los campos son obligatorios");
    return;
  }

  console.log("Enviando formulario");

  // Llama a la función en Supabase para insertar datos
  const respuesta = await consumir_funcion("insert_cliente_formulario", dataContacto);

  // Manejo de la respuesta
  if (respuesta.error) {
    console.error("Error al enviar formulario:", respuesta.error);
    alert("Error al enviar formulario: " + respuesta.error.message);
    return;
  }

  alert("Formulario enviado");
  console.log(dataContacto);
  form.reset();
});

// Selección de elementos de la barra de navegación
const menuToggle = document.querySelector(".menu-toggle");
const menu = document.querySelector(".menu");

// Alternar la visibilidad del menú al hacer clic
menuToggle.addEventListener("click", () => {
    menu.classList.toggle("active");
});
