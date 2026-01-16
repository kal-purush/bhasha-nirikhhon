"use strict";
document.addEventListener("DOMContentLoaded", load);
function load() {
  mostrarReservas();
  let actualizar = document.querySelector("#btn1");
  actualizar.addEventListener("click", traerHotel);
}

function mostrarReservas() {
  console.log(reservas);
  let hotel = document.querySelector(".listaReservas");
  hotel.innerHTML = " ";
  for (let reserva of reservas) {
    let newDiv = document.createElement("div");
    newDiv.className += "hotel";
    let newCont = document.createElement("ul");
    newCont.className += "list-group list-group-flush";
    const listItem = document.createElement("li");
    const title = document.createElement("h5");
    listItem.className += "list-group-item";
    title.textContent = reserva.datos.nombre;
    listItem.appendChild(title);
    const listItem2 = document.createElement("li");
    listItem2.className += "list-group-item";
    listItem2.textContent = reserva.datos.ciudad;
    newCont.appendChild(listItem);
    newCont.appendChild(listItem2);
    newDiv.appendChild(newCont);
    hotel.appendChild(newDiv);
  }
}

let reservas = [
  {
    id: "1",
    datos: {
      nombre: "Hotelito",
      ciudad: "pehuajo",
    },
  },
  {
    id: "2",
    datos: {
      nombre: "Hotelito2",
      ciudad: "pehuajo",
    },
  },
];

function traerHotel() {
  let hotel = {
    nombre: "Hotel Plaza",
    ciudad: "Tandil",
  };
}