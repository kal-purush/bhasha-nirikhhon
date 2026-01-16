
let textoIngreso
let textoSalida=""
let letraEncriptada
let letraPrevia


function textoDeSalida(elemento,texto) {
    let textoSalida2 = document.getElementById(elemento);
    textoSalida2.innerHTML=texto;
}

function encriptar() {


    let textoIngreso = document.getElementById('ingreso').value;

    if (textoIngreso == ""){
        alert('No ha ingresado ningún texto');
        textoDeSalida('tituloEncriptado','Ningún mensaje encontrado')
        textoDeSalida('textoEncriptado','Ingresa el texto que desees encriptar o desencritar')
        location.reload
    }else{
        
        for (let palabra of textoIngreso){
            for (let letra of palabra){
                if (letra == 'a'){
                    letraEncriptada = 'ai';
                }else if(letra == 'e'){
                    letraEncriptada = 'enter';
                }else if(letra == 'i'){
                    letraEncriptada = 'imes';
                }else if(letra == 'o'){
                    letraEncriptada = 'ober';
                }else if(letra == 'u'){
                    letraEncriptada = 'ufat';
                }else{
                    letraEncriptada = letra;

                }
                textoSalida += letraEncriptada + "";
                
            }
        }   textoDeSalida('textoEncriptado',textoSalida);
            let cajaResultado=document.getElementById('resultado');
            let textoSalida2=document.getElementById('textoEncriptado');
            let tituloSalida=document.getElementById('tituloEncriptado');
            let textoMensaje=document.getElementById('textoMensaje');
            let botonCopiar=document.getElementById('copiar');
            let botonEncriptar=document.getElementById('encriptar');
            let imagenBusqueda=document.getElementById('busqueda');
            
            cajaResultado.style.flexDirection='column-reverse';
            textoSalida2.style.textAlign = 'Justify';
            tituloSalida.style.display = 'none';
            textoMensaje.style.display = 'none';
            botonCopiar.style.display = 'block';
            imagenBusqueda.style.display = 'none';
            textoEncriptado.style.display = 'block';
            textoEncriptado.disabled = true;
            botonEncriptar.disabled = true;
            document.getElementById("ingreso").value = "";
            
        
    }
}

function desencriptar() {
    
    textoSalida=""
    textoDeSalida('textoEncriptado',textoSalida)

    let textoIngreso = document.getElementById('ingreso').value;

    if (textoIngreso == ""){
        alert('No ha ingresado ningún texto');
        textoDeSalida('tituloEncriptado','Ningún mensaje encontrado')
        textoDeSalida('textoEncriptado','Ingresa el texto que desees encriptar o desencritar')
        location.reload
    }else{

        for (let palabra of textoIngreso){
            for (let letra of palabra){

                if (letra == 'a'){
                    letraPrevia = letra;
                    letraEncriptada = "";
                }else if(letra == 'e'){
                    letraPrevia = letra;
                    letraEncriptada = "";
                }else if(letra == 'n' && letraPrevia == 'e'){
                    letraEncriptada = 'e'
                    letraPrevia = letra;
                }else if(letra == 'i' && letraPrevia != 'a'){
                    letraPrevia = letra;
                    letraEncriptada = "";
                }else if(letra == 'i' && letraPrevia == 'a'){
                    letraEncriptada = "a";
                    letraPrevia = '';
                }else if(letra == 'm' && letraPrevia == 'i'){
                    letraEncriptada = 'i';
                    letraPrevia = letra;
                }else if(letra == 'o'){
                    letraPrevia = letra;
                    letraEncriptada = "";
                }else if(letra == 'b' && letraPrevia == 'o'){
                    letraEncriptada = "o";
                    letraPrevia = '';
                }else if(letra == 'u'){
                    letraPrevia = letra;
                    letraEncriptada = "";
                }else if(letra == 'f' && letraPrevia == 'u'){
                    letraEncriptada = "u";
                    letraPrevia = '';
                }else if(letra == 'r' && letraPrevia == 'e'){
                    letraEncriptada = "";
                    letraPrevia = '';
                }else if(letra == 't' && letraPrevia != ''){
                    letraEncriptada = "";
                    letraPrevia = '';
                }else if(letra == 's' && letraPrevia == 'e'){
                    letraEncriptada = "";
                    letraPrevia = '';
                }else{
                    letraEncriptada=letra                   
                }
                textoSalida += letraEncriptada + "";

            }
        }

        textoDeSalida('textoEncriptado',textoSalida);
        let cajaResultado=document.getElementById('resultado');
        let textoSalida2=document.getElementById('textoEncriptado');
        let tituloSalida=document.getElementById('tituloEncriptado');
        let textoMensaje=document.getElementById('textoMensaje');
        let botonCopiar=document.getElementById('copiar');
        let botonDesencriptar=document.getElementById('desencriptar');
                
        cajaResultado.style.flexDirection='column-reverse';
        textoSalida2.style.textAlign = 'Justify';
        tituloSalida.style.display = 'none';
        textoMensaje.style.display = 'none';
        botonCopiar.style.display = 'block';
        textoEncriptado.style.display = 'block';
        textoEncriptado.disabled = true;
        botonDesencriptar.disabled = true;
        document.getElementById("ingreso").value = "";

    }

    
}

function copiarTexto() {
    let texto = document.getElementById('textoEncriptado');
    texto.select();
    navigator.clipboard.writeText(texto.value);

}
