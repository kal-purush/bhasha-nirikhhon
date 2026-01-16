/*const button=document.createElement('button');
button.type='button';
button.innerText='wasap';
document.body.appendChild(button);
CON ESTA LINEA DE CODIGOS SE CREA UN BOTON DIRECTAMENTE EN EL HTML, PERO EN NUESTRO HTML TENEMOS QUE USAR
LA ETIQUETA <scrip src="(nombre del js" y su extension .js)></script>*/


/*
function setzo(){
    document.write("hola mundo")
    
}
    */


//EL REAL


var toggle = document.getElementById('container');
var body = document.querySelector('body');
toggle.onclick = function(){
    toggle.classList.toggle('active');
    body.classList.toggle('active');
    
    
}
/*PROYECT "HOLA MUNDO"
function setzo(){
    var contenedor = document.getElementsByClassName("contenedor")[0];
    var toggle = document.getElementById("container")
    if(contenedor.style.visibility == "hidden"){
        toggle.classList.toggle("active");

        contenedor.style.visibility = "visible";

    }else{
        contenedor.style.visibility ="hidden";
    }
}
*/