/*
 * Copyright (c) 2018. Esto está hecho por Alfonso Muñoz Sánchez
 */

var apikey = 'b8187916'
let c;

class Pelicula {
    constructor(poster, title, type, year, ImbdID) {
        this.poster = poster;
        this.title = title;
        this.ImbdID = ImbdID;
        this.year = year


    }
}

class coleccionPeliculas {
    constructor() {
        this.arrayPeliculas = []
    }

    meterPelicula(pelicula) {
        this.arrayPeliculas.push(pelicula)
    }
}

function obtenerPeliculas() {
    var texto = $('input').val()

    $.ajax({
        url: "http://www.omdbapi.com/?s=" + texto + "&apikey=" + apikey,
        success: function (res) {
            var array_res = res;

            crearPeliculas(array_res)
        }
    })
}


function crearPeliculas(peliculas) {
    console.log(peliculas)
    console.log("crearPeliculas")
    console.log(peliculas.Search.length)
    for (let i = 0; i < peliculas.Search.length; i++) {
        console.log(peliculas.Search[i])
        c.meterPelicula(new Pelicula(peliculas.Search[i].Poster, peliculas.Search[i].Title, peliculas.Search[i].Type, peliculas.Search[i].Year, peliculas.Search[i].imbdID))
    }
    enmaquetarPeliculas()


}

function enmaquetarPeliculas() {
    let peliculas = c.arrayPeliculas;
    for (let i = 0; i < peliculas.length; i++) {
        $('#cuerpo').append('<div class="pelicula"><img id="' + peliculas[i].ImdbID + '" src="' + peliculas[i].poster + '"><h2>' + peliculas[i].title + '</h2><p>' + peliculas[i].year + '</p></div>');

    }
}

function limpiarPeliculas() {
    c = new coleccionPeliculas()
    console.log(c.arrayPeliculas)
}

c = new coleccionPeliculas();