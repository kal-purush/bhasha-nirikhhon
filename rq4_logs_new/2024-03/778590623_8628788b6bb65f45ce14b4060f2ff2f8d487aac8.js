// Definir constantes para elementos del DOM
const animalesElement = document.getElementById("Animales");
const animalSelect = document.getElementById("animal");
const edadSelect = document.getElementById("edad");
const comentarios = document.getElementById("comentarios");
const previewImage = document.getElementById("preview");
const btnRegistrar = document.getElementById("btnRegistrar");

fetch('assets/js/animales.json')
  .then(response => response.json())
  .then(data => {
    // Trabaja con los datos del JSON aquí
    data.animales.forEach(animal => {
      agregarAnimal(animal); // Agregar cada animal a la tabla
    });
  })
  .catch(error => console.error('Error al cargar el archivo JSON', error));

// Función para obtener la ruta completa de una imagen
const obtenerRutaImagen = (nombreImagen) => {
  return `assets/${nombreImagen}`; // Ajusta la ruta según la estructura de tu proyecto
};

// Función para obtener la ruta completa de un sonido
const obtenerRutaSonido = (nombreSonido) => {
  return `assets/${nombreSonido}`; // Ajusta la ruta según la estructura de tu proyecto
};

// Función para crear una tarjeta de animal
const crearTarjetaAnimal = (animal) => {
  const tarjeta = document.createElement("div");
  tarjeta.classList.add("card", "bg-dark", "text-white", "shadow-lg", "m-2");
  tarjeta.style.width = "18rem";

  const imagen = document.createElement("img");
  imagen.classList.add("card-img-top");
  imagen.src = obtenerRutaImagen(animal.imagen);
  imagen.alt = `Imagen de un ${animal.name}`;

  const cardBody = document.createElement("div");
  cardBody.classList.add("card-body");

  const titulo = document.createElement("h5");
  titulo.classList.add("card-title");
  titulo.textContent = animal.name;

  const subtitulo = document.createElement("h6");
  subtitulo.classList.add("card-subtitle", "mb-2", "text-muted");
  subtitulo.textContent = `Edad: ${animal.edad || "No indicada"}`;

  const texto = document.createElement("p");
  texto.classList.add("card-text");
  texto.textContent = animal.comentarios;

  const btnSonido = document.createElement("button");
  btnSonido.classList.add("btn", "btn-primary", "mt-2");
  btnSonido.textContent = "Reproducir sonido";
  btnSonido.addEventListener("click", () => reproducirSonido(animal.sonido));

  cardBody.appendChild(titulo);
  cardBody.appendChild(subtitulo);
  cardBody.appendChild(texto);
  cardBody.appendChild(btnSonido);

  tarjeta.appendChild(imagen);
  tarjeta.appendChild(cardBody);

  return tarjeta;
};

// Función para agregar un animal a la tabla
const agregarAnimal = (animal) => {
  const tarjetaAnimal = crearTarjetaAnimal(animal);
  animalesElement.appendChild(tarjetaAnimal);
};

// Función para reproducir un sonido
const reproducirSonido = (nombreSonido) => {
  const audio = new Audio(obtenerRutaSonido(nombreSonido));
  audio.play();
};

// Función para manejar el registro de un nuevo animal
const registrarAnimal = () => {
  const animalSeleccionado = animalSelect.value;
  const edadSeleccionada = edadSelect.value;
  const comentariosAnimal = comentarios.value;

  // Validar que todos los campos estén completos
  if (animalSeleccionado === "" || edadSeleccionada === "" || comentariosAnimal === "") {
    alert("Por favor, complete todos los campos del formulario.");
    return;
  }

  // Crear un nuevo objeto animal con los datos seleccionados
  const nuevoAnimal = {
    name: animalSeleccionado,
    imagen: `${animalSeleccionado}.png`,
    sonido: `${animalSeleccionado}.mp3`,
    edad: edadSeleccionada,
    comentarios: comentariosAnimal
  };

  // Agregar el nuevo animal a la tabla
  agregarAnimal(nuevoAnimal);

  // Limpiar los campos del formulario
  animalSelect.value = "";
  edadSelect.value = "";
  comentarios.value = "";

  // Actualizar la imagen previa con la imagen del animal seleccionado (opcional)
  previewImage.src = obtenerRutaImagen(`${animalSeleccionado}.png`);
};

// Agregar event listener al botón de registrar
btnRegistrar.addEventListener("click", registrarAnimal);

// Agregar event listener al selector de animales para actualizar la imagen previa (opcional)
animalSelect.addEventListener("change", () => {
  previewImage.src = obtenerRutaImagen(`${animalSelect.value}.png`);
});