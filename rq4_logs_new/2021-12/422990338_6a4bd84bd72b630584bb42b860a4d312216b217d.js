/*

Formato: código fuente en JavaScript
Sublime Text o VisualStudio.  
Sugerencia: ten en cuenta que los métodos de jQuery show, hide, fadeIn, fadeOut, slideUp, slideDown y toggle ofrecen animaciones con comportamiento predefinido. Si quieres armar tus propias animaciones, deberás usar animate.
También recuerda que los métodos jQuery pueden encadenarse: 
http://www.w3big.com/es/jquery/jquery-chaining.html

>> Consigna: codifica animaciones concatenadas sobre uno o más elementos. Es decir que luego de finalizar una animación en su función callback, se especifica la llamada a otra animación.
>>Aspectos a incluir en el entregable:
Archivo HTML y archivo JavaScript referenciado, que incluya la definición de dos o más animaciones y sus respectivas funciones callback.
>>Ejemplo de secuencia de animación:
1) Mostrar un elemento con fadeIn() y al concluir su transición, ocultar otro elemento con fadeOut(). 
2) Aumentar el margen de un elemento con animate() y al concluir su transición, disminuir el margen del mismo elemento con animate().
3) Disminuir la altura de un elemento con animate() y al concluir su transición, esperar con delay() unos ms, y volver al tamaño original.


*/



//uso de método ready de jquery
$(() => {
    console.log('El DOM esta listo');
});

//uso de animación Jquery
$('#mostrarCarousel').on('click', function () {
    $("#carouselExampleDark").fadeIn(3000);
});


/*
$("#mostrarCarousel").click(function () { 
    $("#carouselExampleDark").show();
});
*/


const carro = [];

let miFormulario = $('#formulario');//selector jquery
miFormulario.submit(function (e) { //uso de evento jquery

    e.preventDefault();

    let formulario = e.target;

    let numeroPasajeros = formulario.children[1].value;
    let numeroIsla = formulario.children[5].value;
    let numeroServicio = formulario.children[9].value;

    //console.log(numeroPasajeros);
    //console.log(numeroIsla);
    //console.log(numeroServicio);

    carro.push(numeroPasajeros); 
    carro.push(numeroIsla);
    carro.push(numeroServicio);

    console.log(carro);

    class isla {
        constructor(id , nombre , precio) {
            this.id = id;
            this.nombre = nombre;
            this.precio = precio;
        }
    
        sumaTasas(){
            this.precio = (this.precio * numeroPasajeros * 1.12).toFixed(0); //pasajeros input
        }
    
    }
    
    const islas =[];
    
    islas.push(new isla(1 , "Ibiza y Formentera" , 650));
    islas.push(new isla(2 ,"Mallorca" , 900));
    islas.push(new isla(3 , "Menorca" , 750));
    
    for (const isla of islas){
        isla.sumaTasas();
    }
    
    //console.log(islas);
    
    class servicio {
        constructor(id , nombre , precio) {
            this.id = id;
            this.nombre = nombre;
            this.precio = precio;
        }
    
        sumaIVA(){
            this.precio = (this.precio * numeroPasajeros * 1.19).toFixed(0); //pasajeros input
        }
    }
    
    const servicios = [];
    
    
    servicios.push(new servicio(1 , "Pick-up ida y vuelta al aeropuerto" , 50));
    servicios.push(new servicio(2 , "Tarde de Buceo" , 120));
    servicios.push(new servicio(3 , "Stand up paddle por 2 horas" , 35));
    servicios.push(new servicio(4 , "ningún servicio extra" , 0)); 
    
    for(const servicio of servicios){
        servicio.sumaIVA();
    }
    
    //console.log(servicios);
    
    
    class totalCompra{
        constructor(destino, extra){
            this.destino = destino;
            this.extra = extra;
    
        }
            
    }
    

    let encontradoIsla = islas.find(isla => isla.id == numeroIsla); // input isla
    let encontradoServicio = servicios.find(servicio => servicio.id == numeroServicio); //input servicio
    
    let precioTotal = parseInt(encontradoIsla.precio) + parseInt(encontradoServicio.precio);
    
    
    function cotizar() {
    

    
        //console.log(encontradoIsla.precio); 
        //console.log(encontradoServicio.precio);
    

    
        //console.log(precioTotal);

    }
    
    cotizar();
    
    
    
    //generar html a partir de un array:

    function mensajeFinal () {
      // crea un nuevo div con método append de Jquery y uso de animaciones


      $("#cotizar").append(`<div id ="newDiv"><p>Cotizaste un viaje a ${encontradoIsla.nombre} para ${numeroPasajeros} personas, contratando ${encontradoServicio.nombre} por un total de USD$ ${precioTotal}  .\n Para más información llena el formulario de contacto y nos comunicaremos a la brevedad contigo</p></div>`);

      /*
      $("#newDiv").css("color", "#2284E6"); //uso de método css de jquery
      $("#newDiv").css("fontSize", "1.5rem");
      $("#newDiv").css("padding", "4rem");
      */
      $("#newDiv").css("color", "#2284E6")
                  .css("fontSize", "1.5rem")
                  .css("padding", "4rem")
                  .slideUp(10)
                  //.delay("fast")
                  .slideDown(4000);
 
                  

      
      //opcion.appendChild(newDiv); //añade texto al div creado.
    
      // añade el elemento creado y su contenido al DOM
     // let currentDiv = document.getElementById("imagenCotizar");
      //document.body.insertBefore(newDiv, currentDiv);
    }
    
    mensajeFinal ();

    /*
    function mensajeFinal () {
      // crea un nuevo div
      // y añade contenido
      let opcion = document.getElementById("cotizar");// acá hice un cambio
      let newDiv = document.createElement("div");
      newDiv.innerHTML = `<p>Cotizaste un viaje a ${encontradoIsla.nombre} para ${numeroPasajeros} personas, contratando ${encontradoServicio.nombre} por un total de USD$ ${precioTotal}  .\n Para más información llena el formulario de contacto y nos comunicaremos a la brevedad contigo</p>`;
      newDiv.style.color = "blue";
      newDiv.style.fontSize = "1.5rem";
      newDiv.style.padding = "4rem";
      
      opcion.appendChild(newDiv); //añade texto al div creado.
    
      // añade el elemento creado y su contenido al DOM
      let currentDiv = document.getElementById("imagenCotizar");
      document.body.insertBefore(newDiv, currentDiv);
    }
    
    mensajeFinal ();

    */
    




}); //uso de eventos



// Me sale e siguiente error en la consola que no entiendo, porque el html que genero igual se muestra en la página:  main.js:142 Uncaught DOMException: Failed to execute 'insertBefore' on 'Node': The node before which the new node is to be inserted is not a child of this node. at mensajeFinal (file:///C:/proyectos_coder/proyectojs/proyectofinaljs/js/main.js:142:21) at HTMLFormElement.validarFormulario (file:///C:/proyectos_coder/proyectojs/proyectofinaljs/js/main.js:145:5)


const clientes = [];


/*
let contacto = document.getElementById('contacto');
contacto.addEventListener('submit', guardarBase);

function guardarBase(){

    let name = document.getElementById('name');
    let lastName = document.getElementById('lastName');
    let inputEmail4 = document.getElementById('inputEmail4');
    let inputPhone = document.getElementById('inputPhone');

    localStorage.setItem( name , lastName ,inputEmail4 , inputPhone);
}

*/

let contacto = $('#contacto'); //uso de selector jquery
contacto.submit(function (e) { //uso de evento jquery

    e.preventDefault();

    let formulario = e.target;

    let name = formulario.children[1].value;
    let lastName = formulario.children[5].value;
    let inputEmail4 = formulario.children[9].value;
    let inputPhone = formulario.children[13].value;

    //console.log(name);
    //console.log(lastName);
    //console.log(inputEmail4);
    //console.log(inputPhone);

    clientes.push(name); 
    clientes.push(lastName);
    clientes.push(inputEmail4);
    clientes.push(inputPhone);

    //console.log(clientes);

    localStorage.setItem('baseClientes' , JSON.stringify(clientes));

    let base = localStorage.getItem('baseClientes');

    base = JSON.parse(base);

    console.log(base);
    
})




//API geolocalización

let ubicacion = navigator.geolocation.getCurrentPosition( mostrarUbicacion);

function mostrarUbicacion ( position ) {

    console.log(position.coords.latitude);
    console.log(position.coords.longitude);
    console.log(position.coords.latitude , position.coords.longitude);
    
}

//$("#climaIbiza").on("click" , mostrarUbicacion); no se escribe este click porque funcion tiene un callbck especial que lo muestra

//API CLIMA

let urlClimaIbiza = "http://api.openweathermap.org/data/2.5/weather?q=Ibiza&units=metric&appid=85af83e8354fd0cc1b31c0b7abbbf37a&lang=es";

$("#climaIbiza").click(function(){

    $.get( urlClimaIbiza , function(data){
        
        console.log(data);
        console.log(data.weather[0].description);
        let iconoClima = data.weather[0].icon;
        let iconoUrl = "http://openweathermap.org/img/wn/" + iconoClima + "@2x.png";
        

        let contenido = `<div>
                            <h3>${data.name}</h3>
                            <h3>${data.main.temp.toFixed(0)}°C</h3>
                            <img src="${iconoUrl}">
                            <p>Pronóstico: ${data.weather[0].description}</p>
                            <p>Temp máx: ${data.main.temp_max.toFixed(0)}°C</p>
                            <p>Temp mín: ${data.main.temp_min.toFixed(0)}°C</p>   
                        </div>`
        
             $("#formentera").append(contenido);   

 })

})


/*
let climaAJAX = $.ajax({

    url: "http://api.openweathermap.org/data/2.5/weather",
    type: "GET",
    data:{
        q:'Ibiza',
        appid: '85af83e8354fd0cc1b31c0b7abbbf37a',
        dataType: 'json',
        units: "metric"
    },

    success: function(data){

        console.log(data);

    }

})

console.log(climaAJAX);

*/


let urlClimaFormentera = "http://api.openweathermap.org/data/2.5/weather?lat=38.7191&lon=1.4587&units=metric&appid=85af83e8354fd0cc1b31c0b7abbbf37a&lang=es";

$("#climaFormentera").click(function(){

    $.get( urlClimaFormentera , function(data){
        
        console.log(data);
        console.log(data.weather[0].description);
        let iconoClima = data.weather[0].icon;
        let iconoUrl = "http://openweathermap.org/img/wn/" + iconoClima + "@2x.png";
        

        let contenido = `<div>
                            <h3>${data.name}</h3>
                            <h3>${data.main.temp.toFixed(0)}°C</h3>
                            <img src="${iconoUrl}">
                            <p>Pronóstico: ${data.weather[0].description}</p>
                            <p>Temp máx: ${data.main.temp_max.toFixed(0)}°C</p>
                            <p>Temp mín: ${data.main.temp_min.toFixed(0)}°C</p>   
                        </div>`
        
             $("#formentera").append(contenido);   

 })

})



let urlClimaMallorca = "http://api.openweathermap.org/data/2.5/weather?q=Palma&units=metric&appid=85af83e8354fd0cc1b31c0b7abbbf37a&lang=es";

$("#climaMallorca").click(function(){

    $.get( urlClimaMallorca , function(data){
        
        console.log(data);
        console.log(data.weather[0].description);
        let iconoClima = data.weather[0].icon;
        let iconoUrl = "http://openweathermap.org/img/wn/" + iconoClima + "@2x.png";
        

        let contenido = `<div>
                            <h3>${data.name}</h3>
                            <h3>${data.main.temp.toFixed(0)}°C</h3>
                            <img src="${iconoUrl}">
                            <p>Pronóstico: ${data.weather[0].description}</p>
                            <p>Temp máx: ${data.main.temp_max.toFixed(0)}°C</p>
                            <p>Temp mín: ${data.main.temp_min.toFixed(0)}°C</p>   
                        </div>`
        
             $("#mallorca").append(contenido);   

 })

})



let urlClimaMenorca = "http://api.openweathermap.org/data/2.5/weather?lat=39.9997&lon=3.835&units=metric&appid=85af83e8354fd0cc1b31c0b7abbbf37a&lang=es";

$("#climaMenorca").click(function(){

    $.get( urlClimaMenorca , function(data){
        
        console.log(data);
        console.log(data.weather[0].description);
        let iconoClima = data.weather[0].icon;
        let iconoUrl = "http://openweathermap.org/img/wn/" + iconoClima + "@2x.png";
        

        let contenido = `<div>
                            <h3>${data.name}</h3>
                            <h3>${data.main.temp.toFixed(0)}°C</h3>
                            <img src="${iconoUrl}">
                            <p>Pronóstico: ${data.weather[0].description}</p>
                            <p>Temp máx: ${data.main.temp_max.toFixed(0)}°C</p>
                            <p>Temp mín: ${data.main.temp_min.toFixed(0)}°C</p>   
                        </div>`
        
             $("#menorca").append(contenido);   

 })

})



