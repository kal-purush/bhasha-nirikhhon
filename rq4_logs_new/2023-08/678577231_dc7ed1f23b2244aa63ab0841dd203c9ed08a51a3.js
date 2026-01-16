document.addEventListener("DOMContentLoaded", () => {
  const enlacesNavegacion = document.querySelectorAll(
    ".barra-navegacion__ul a"
  );

  enlacesNavegacion.forEach((enlace) => {
    enlace.addEventListener("click", cargarSeccion);
  });
});

function cargarSeccion(event) {
  event.preventDefault();

  const enlace = event.target.getAttribute("href");
  const contenido = document.getElementById("sec");

  if(enlace === "index.html"){
    return;
  }

  fetch(enlace)
    .then((response) => response.text())
    .then((html) => {
      contenido.innerHTML = html;
    });
}