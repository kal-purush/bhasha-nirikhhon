import { obtenerPeliculasDeLS } from "../utils.js";

const $contenedorSeries = document.querySelector("#contenedorSeries");

const listaPeliculas = obtenerPeliculasDeLS();
const listaAMostrar = [];

listaPeliculas.forEach((pelicula) => {
  if (pelicula.tipo === "pelicula") {
    listaAMostrar.push(pelicula);
  }
});

listaAMostrar.forEach((serie) => {
  console.log(serie);
  const $article = document.createElement("article");
  const $imgPelicula = document.createElement("img");
  const $linkPelicula = document.createElement("a");
  $linkPelicula.href = `./ver.html?codigo=${serie.codigo}`;

  $article.classList.add("cardPelicula", "col-lg-3", "col-md-6", "mb-3");

  $imgPelicula.classList.add("imgPelicula");
  $imgPelicula.src = `${serie.caratula}`;
  $linkPelicula.appendChild($imgPelicula);
  $article.appendChild($linkPelicula);
  $contenedorSeries.appendChild($article);
});