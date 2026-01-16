const mainCanvas = document.getElementById("main-canvas");
const canvasComparar=document.getElementById('canvas-comparar')
const crearFirma = document.getElementById("crear-firma");
const context = mainCanvas.getContext("2d");
const context2=canvasComparar.getContext('2d');
const btnGuardar = document.querySelector(".btn-send");
const btnGuardarComp = document.querySelector(".btn-send-comparar");
const btnEnviar = document.querySelector("#btn-enviar");
const btnComparar = document.querySelector("#btn-comparar");
const formulario = document.querySelector(".formulario");
const firma = document.getElementById("firma-input");
const comparaFirma = document.querySelector(".comparar-firma");
const msgCoincide=document.querySelector('.msg-coincide')
const msgNoCoincide=document.querySelector('.msg-noCoincide')
const formCompFirma=document.querySelector('.form-comp-firma');
const compFirma=document.querySelector('#comp-firma');

//Campos del formulario

const nombre = document.getElementById("nombre");
const apellido = document.getElementById("apellido");
const email = document.getElementById("email");

let idUser = "";
let comparar = false;

context.fillStyle = "white";
context.fillRect(0, 0, mainCanvas.width, mainCanvas.height);
let initialX;
let initialY;
context2.fillStyle = "white";
context2.fillRect(0, 0, canvasComparar.width, canvasComparar.height);
let initialX2;
let initialY2;

const dibujar = (cursorX, cursorY) => {
    context.beginPath();
  context.moveTo(initialX, initialY);
  context.lineWith = 30;
  context.strokeStyle = "#000";
  context.lineCap = "round";
  context.lineJoin = "round";
  context.lineTo(cursorX, cursorY);
  context.stroke();
  initialX = cursorX;
  initialY = cursorY;
  
};

const dibujar2=(cursorX,cursorY)=>{
  context2.beginPath();
  context2.moveTo(initialX2, initialY2);
  context2.lineWith = 30;
  context2.strokeStyle = "#000";
  context2.lineCap = "round";
  context2.lineJoin = "round";
  context2.lineTo(cursorX, cursorY);
  context2.stroke();
  initialX2 = cursorX;
  initialY2 = cursorY;
}

const mouseDown = (e) => {
  initialX = e.offsetX;
  initialY = e.offsetY;
  dibujar(initialX, initialY);
  mainCanvas.addEventListener("mousemove", mouseMoving);
};

const mouseMoving = (e) => {
  dibujar(e.offsetX, e.offsetY);
};

const mouseUp = () => {
  mainCanvas.removeEventListener("mousemove", mouseMoving);
};


//canvas

const mouseDown2 = (e) => {
  initialX2 = e.offsetX;
  initialY2 = e.offsetY;
  dibujar2(initialX2, initialY2);
  canvasComparar.addEventListener("mousemove", mouseMoving2);
};

const mouseMoving2 = (e) => {
  dibujar2(e.offsetX, e.offsetY);
};

const mouseUp2 = () => {
  canvasComparar.removeEventListener("mousemove", mouseMoving2);
};

mainCanvas.addEventListener("mousedown", mouseDown);
mainCanvas.addEventListener("mouseup", mouseUp);

//canvas 2
canvasComparar.addEventListener("mousedown", mouseDown2);
canvasComparar.addEventListener("mouseup", mouseUp2);

const enviarImagen = async () => {
  const fd = new FormData();
  fd.append("nombre", nombre);
  fd.append("apellido", apellido);
  fd.append("email", email);
  fd.append("firma", firma.files[0]);
  console.log(firma.files[0]);

  const rta = axios
    .post("http://127.0.0.1:8000/api/users/create", fd)
    .then((res) => {
      idUser = res.data.id;
    });

  comparar = true;
  comparaFirma.style.display = "block";
  formulario.style.display = "none";
  
};

const compararImagen = () => {
  const fd = new FormData();
  fd.append("user", idUser);
  fd.append("firma", firma.files[0]);
  const rta = axios({
    method: "post",
    headers: {
      "Content-Type": "multipart/form-data",
    },
    url: "http://127.0.0.1:8000/file",
    data: fd,
  }).then((res) => {
    console.log(res.data);
    if (res.data == 0) {
      console.log("No coincide");
      msgNoCoincide.style.display='block'
    } else {
      console.log("Coincide");
      msgCoincide.style.display='block'
    }
  });
};

const descargarImagen = () => {
  
  let enlace = document.createElement("a");
  // El título
  if (!comparar) {
    enlace.download = "firma.jpg";
    // Convertir la imagen a Base64 y ponerlo en el enlace
  enlace.href = mainCanvas.toDataURL("image/jpeg", 1);
  //context.clearRect(0, 0, mainCanvas.width, mainCanvas.height);
  context.fillStyle = "white";
  // Hacer click en él
  enlace.click();
  } else {
    enlace.download = "firmaComparar.jpg";
    // Convertir la imagen a Base64 y ponerlo en el enlace
  enlace.href = canvasComparar.toDataURL("image/jpeg", 1);
  //context.clearRect(0, 0, mainCanvas.width, mainCanvas.height);
  context.fillStyle = "white";
  // Hacer click en él
  enlace.click();
  }
  

  if (!comparar) {
    crearFirma.style.display = "none";
    formulario.style.display = "block";
  } else {
    console.log('comparando');
    compFirma.style.display='none'
    formCompFirma.style.display = "block";
  }
  
};

btnGuardar.addEventListener("click", descargarImagen);
btnGuardarComp.addEventListener('click',descargarImagen)
btnEnviar.addEventListener("click", enviarImagen);
btnComparar.addEventListener('click',compararImagen)