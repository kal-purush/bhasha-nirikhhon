// LLAMADO A ELEMENTOS HTML

//CONTENEDORES
let modalTarjeta = document.getElementById('modalInfo')
let contenedorTarjeta = document.getElementById('contenedorTarjetas')
let modalVisible = document.getElementById('modalFondo')
let contador = document.getElementById('totalPagina')

// BOTONES FILTROS
let btnFiltroAll=document.getElementById('btnFiltroTodos')
let btnFiltroMen=document.getElementById('btnFiltroHombre')
let btnFiltroWomen=document.getElementById('btnFiltroMujer')
let btnFiltroUnk=document.getElementById('btnFiltroUnknown')
let btnFiltroGender=document.getElementById('btnFiltroGenderless')
// PAGINADO
let btnPaginadoPrimer = document.getElementById('btnPrimerPagina')
let btnPaginadoAnterior = document.getElementById('btnAnteriorPagina')
let btnPaginadoSiguiente = document.getElementById('btnSiguientePagina')
let btnPaginadoUltima = document.getElementById('btnUltimaPagina')
// BOTON MODAL
let btnVerMas = document.getElementById('btnModal')
let idpersonaje;
let paginaActual = 1
let totalPersonajes;
solicitudFetch(paginaActual)
let totalPagina;

//Funciones 

//Fetch y datos
function solicitudFetch(paginaActual){
    fetch('https://rickandmortyapi.com/api/character/?page='+paginaActual)
    .then( (data) => {
        return data.json()
    }).then( (data) => {
        totalPersonajes = data.results
        mostrarPersonajes(totalPersonajes)
    })
}

function mostrarModal() {
    modalVisible.style.display = 'block';
    fetch('https://rickandmortyapi.com/api/character/' + idpersonaje)
        .then((dataUnico) => {
            return dataUnico.json();
        })
        .then((dataUnico) => {
            listarEpisodios(dataUnico.episode)
                .then((episodios) => {
                    modalTarjeta.innerHTML = `
                        <div class="modalSuperior">
                        <img src="${dataUnico.image}" alt="" class='imgModal'>

                            <div class="contenedorDato">
                                <h3 class="tituloDato">NAME</h3>
                                <p class="infoPersonajeModal">${dataUnico.name}</p>
                            </div>

                            <div class="contenedorDato">
                                <h3 class="tituloDato">GENDER</h3>
                                <p class="infoPersonajeModal">${dataUnico.gender}</p>
                            </div>

                            <div class="contenedorDato">
                                <h3 class="tituloDato">STATUS</h3>
                                <p class="infoPersonajeModal">${dataUnico.status}</p>
                            </div>
                            <div class="contenedorDato">
                                <h3 class="tituloDato">TYPE</h3>
                                <p class="infoPersonajeModal">${dataUnico.type}</p>
                            </div>
                            </div>
                            <div class="modalInferior">
                            <div class="contenedorDato">
                                <h3 class="tituloDato">SPECIE</h3>
                                <p class="infoPersonajeModal">${dataUnico.species}</p>
                            </div>
                            <div class="contenedorDato">
                                <h3 class="tituloDato">ORIGEN</h3>
                                <p class="infoPersonajeModal">${dataUnico.origin.name}</p>
                            </div>
                            <div class="contenedorDato">
                                <h3 class="tituloDato">LOCATION</h3>
                                <p class="infoPersonajeModal">${dataUnico.location.name}</p>
                            </div>
                            <div class="contenedorDato">
                                <h3 class="tituloDato">EPISODES</h3>
                            <p class="infoPersonajeModal episodios" >${episodios}</p>
                            </div>
                        </div>
                    `;
                });
        });
}

function mostrarPersonajes(lista){
    contenedorTarjeta.innerHTML=''
    lista.forEach((personajeObj) =>{
    contenedorTarjeta.innerHTML+=`
        <div class="card">
            <img src="${personajeObj.image}" alt="" class="imgPersonaje">
            <h3 class="nombre">${personajeObj.name} </h3>
            <p class="info">Gender: ${personajeObj.gender}</p>
            <p class="info">Status: ${personajeObj.status}</p>
            <p class="info">Specie: ${personajeObj.species}</p>
            <p class="info">Location: ${personajeObj.location.name}</p>
            <p class="info">Origin: ${personajeObj.origin.name}</p>
            <button class="botonMas" id="btnModal" data-id="${personajeObj.id}">Ver más</button>
        </div> `
    });
    // CAPTURA DE ID PARA MODAL
    let botonesVerMas = document.querySelectorAll('.botonMas'); // SELECCIONA TODOS LOS BOTONES CON CLASE .botonMas
    botonesVerMas.forEach((boton) => {
        boton.addEventListener('click', () => { // ASIGNA A CADA BOTON UN EVENTO
            idpersonaje = boton.getAttribute('data-id'); //CAPTURA EL DATO PARA IMPRIMIRLO EN EL MODAL
            mostrarModal()
        });
    });
    totalPagina = lista.length //CAPTURA TOTAL DE ELEMENTOS EN PANTALLA
    mostrarTotal() 
    habilitarBotones() 
}

// -------------------------FUNCIONES COMPLEMENTARIAS---------------------------------------------//

//-----LISTADO DE EPISODIOS (PARA MODAL)
function listarEpisodios(lista) {
    const promesasEpisodios = lista.map((episodioURL) => { 
        return fetch(episodioURL).then((response) => response.json());
    }); //realiza un fetch de todas las url

    return Promise.all(promesasEpisodios) //devuelve los datos cuando todas las promesas estan cumplidas
        .then((episodios) => {
            return episodios.map((datoEpisodio) => {
                return `Nombre: ${datoEpisodio.name},Fecha de lanzamiento: ${datoEpisodio.air_date},Nro: ${datoEpisodio.episode}<br>`;
            }).join('<br>');
        })
}




//---------TOTAL DE PERSONAJES EN PANTALLA
function mostrarTotal(){
    if(totalPagina>1 || totalPagina<1){
    contador.innerText=`Mostrando ${totalPagina} personajes`
}else{
    contador.innerText=`Mostrando ${totalPagina} personaje`
    }
}
//---CERRAR MODAL
function cerrarModal(){
    modalVisible.style.display='none'
}


//------------------------------Filtros
function filtroMujeres(){
    let mujeres= totalPersonajes.filter((personaje)=>{
        return personaje.gender === 'Female'
    })
    mostrarPersonajes(mujeres)
    mostrarTotal() 

}
function filtroHombres(){
    let hombres= totalPersonajes.filter((personaje)=>{
        return personaje.gender === 'Male'
    })
    mostrarPersonajes(hombres) 
    mostrarTotal()

}
function filtroUnknown(){
    let desconocido= totalPersonajes.filter((personaje)=>{
        return personaje.gender === 'unknown'
    })
    mostrarPersonajes(desconocido) 
    mostrarTotal()
}
function filtroGenderless(){
    let sinGenero = totalPersonajes.filter((personaje)=>{
        return personaje.gender === 'Genderless';
    })
    mostrarPersonajes(sinGenero);
    mostrarTotal()

}
function filtroTodos(){
    mostrarPersonajes(totalPersonajes)    
    mostrarTotal()
}
//-------------------Paginado----------------------------

function siguientePagina(){
    paginaActual++;
    solicitudFetch(paginaActual)
    
}
function anteriorPagina(){
    paginaActual--;
    solicitudFetch(paginaActual)
}
function ultimaPagina(){
    paginaActual=42;
    solicitudFetch(paginaActual)
}
function primerPagina(){
    paginaActual=1;
    solicitudFetch(paginaActual)
}

function habilitarBotones(){
    if (paginaActual === 42){
        btnPaginadoSiguiente.disabled=true
        btnPaginadoUltima.disabled=true
    }else{
        btnPaginadoSiguiente.disabled=false
        btnPaginadoUltima.disabled=false
    }
    if (paginaActual === 1){
        btnPaginadoPrimer.disabled=true
        btnPaginadoAnterior.disabled=true
    }else{
        btnPaginadoPrimer.disabled=false
        btnPaginadoAnterior.disabled=false
    }
}



// ----------------------------------EVENTOS----------------------------- //

//FILTROS----------------------------------------------------------
btnFiltroMen.addEventListener('click',filtroHombres)
btnFiltroWomen.addEventListener('click',filtroMujeres)
btnFiltroUnk.addEventListener('click',filtroUnknown)
btnFiltroGender.addEventListener('click',filtroGenderless)
btnFiltroAll.addEventListener('click',filtroTodos)
//PAGINADO------------------------------------------------------
btnPaginadoPrimer.addEventListener('click',primerPagina)
btnPaginadoAnterior.addEventListener('click',anteriorPagina)
btnPaginadoSiguiente.addEventListener('click',siguientePagina)
btnPaginadoUltima.addEventListener('click',ultimaPagina)
modalVisible.addEventListener('click',cerrarModal)