let nombre = document.getElementById('nombre')
let correo = document.getElementById('correo')
let error = document.getElementById('error')
error.style.color = 'red'


function enviarFormulario(){
    console.log(`error al enviar formulario${error}`)

    let mensajesError = [];

    if(nombre.value === null || nombre.value === ''){
        mensajesError.push('Ingresa tu nombre')
    }
    
    if(correo.value === null || correo.value === ''){
        mensajesError.push('Ingresa tu correo')
    }

    if(mensaje.value === null || mensaje.value === ''){
        mensajesError.push('Ingresa tu mensaje')
    } 
    
    Swal.fire({
        icon: 'error',
        title: 'Error en los datos ingresados',
        text: error.innerHTML = mensajesError.join(', ')
      })


    return false;
}