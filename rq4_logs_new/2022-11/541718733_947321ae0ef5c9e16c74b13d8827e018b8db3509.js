const miCheckbox = document.getElementById("exampleCheck1");
const formulario = document.querySelector("#formulario");
formulario.addEventListener("submit", (e) => {
  console.log("Ejecutando");
  console.log("Click en enviar");
  e.preventDefault();

  const datos = new FormData(formulario);

  if (miCheckbox.checked) {
    console.log("campo email:", datos.get("emailCampo"));
    console.log("campo contraseña:", datos.get("passCampo"));
    console.log("campo contraseña on");
  }
  formulario.reset();
});