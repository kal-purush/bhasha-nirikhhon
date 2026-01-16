const quill = new Quill("#editor", {
    theme: "snow",
    placeholder: "Escribe el contenido aquí...",
    modules: { toolbar: true },
  });
  
  function guardarPublicacion(event) {
    event.preventDefault();
  
    const contenido = quill.root.innerHTML;
    document.getElementById("contenido").value = contenido;
  
    const form = document.getElementById("postForm");
    const formData = new FormData(form);
  
    try {
      fetch("/save-content", {
        method: "POST",
        body: formData,
      });
  
      console.log("Publicación enviada correctamente");
      globalThis.location.href = "/";
    } catch (error) {
      console.error("Error al enviar la publicación:", error);
    }
  }
  
  document.addEventListener("DOMContentLoaded", () => {
    const saveButton = document.getElementById("saveButton");
    saveButton.addEventListener("click", guardarPublicacion);
  });