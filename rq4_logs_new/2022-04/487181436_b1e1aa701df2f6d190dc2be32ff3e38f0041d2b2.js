console.log("hola");

const form = document.querySelector("#form");

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const usuario = document.querySelector("#usuario").value;
  

  console.log(usuario);
});

const nada = () => {};