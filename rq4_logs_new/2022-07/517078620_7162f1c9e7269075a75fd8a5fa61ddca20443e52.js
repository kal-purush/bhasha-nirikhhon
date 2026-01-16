let ubicacionP = window.scrollY;
let $nav = document.querySelector('#navP');

window.addEventListener("scroll", function () {
    //Ocultamos
    let ubicacionAct = window.scrollY;

    if (ubicacionP >= ubicacionAct) {
        $nav.style.top = "0px";
    } else {
        $nav.style.top = "-7em"
        if( !($("#botonCollapse").hasClass("collapsed")) ){
            $("#botonCollapse").trigger("click");
        }
    }
    ubicacionP = ubicacionAct;
    //Seleccionamos componentes
    var header = document.querySelector("nav");

    //Cambiamos el estilo
    header.classList.toggle("fijo", window.scrollY > 0);
});