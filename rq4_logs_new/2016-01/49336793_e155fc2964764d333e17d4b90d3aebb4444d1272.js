function validarFormulario(){
	var verificar=true;
	
	var expRegEmail = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,4})+$/;
	
	var nombre = document.getElementById("nombre")
	var telefono = document.getElementById("telefono")
	var mail = document.getElementById("mail")
	var mensaje = document.getElementById("mensaje");

	
	if(!nombre.value){
	alert("Escriba su nombre por favor.");
	nombre.focus();
	verificar=false;
	return false;
	}
	else if(!telefono.value){
	alert("Escriba su telefono por favor.");
	telefono.focus();
	verificar=false;
	return false;
	}
	else if(!email.value){
	alert("Escriba su email");
	email.focus();
	email.focus();
	verificar=false;
	return false;
	}
	else if(!expRegEmail.exec(email.value)){
	alert("Escriba un email valido por favor.");
	email.focus();
	email.focus();
	verificar=false;
	return false;
	}else if(!mensaje.value){
			alert("Escriba un mensaje");
			mensaje.focus();
			verificar = false;
			return false;
	}

	

	if(verificar==true){
	document.formulario.submit();
	}
}
function limpiarFormulario(){
	document.getElementById("formulario").reset();
}

window.onload=function(){
	var botonLimpiar=document.getElementById("limpiar");
	botonLimpiar.onclick=limpiarFormulario;
	var botonEnviar=document.getElementById("enviar");
	botonEnviar.onclick=validarFormulario;
}
