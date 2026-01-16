
/*=============================================
VALIDAR CEDULA EXISTENTE AJAX
=============================================*/
$("#cedulaHistoria").change(function(){

	var cedula = $("#cedulaHistoria").val();

	var datos = new FormData();
	datos.append("validarCedula", cedula);
	
	$.ajax({
		url:"views/modules/ajax.php",
		method:"POST",
		data: datos,
		cache: false,
		contentType: false,
		processData: false,
		success:function(respuesta){
			
			if(respuesta == 0){

				
				$("label[for='validarRegistro'] span").html("");

                correoExistente = false;
			}

			else{

                $("label[for='validarRegistro'] span").html('<p>La Cedula No se encuentra registrada</p>');

               
                correoExistente = true;

			}
		
		}

	});

});

/*=====  FIN VALIDAR USUARIO EXISTENTE AJAX  ======*/



/*=============================================
VALIDAR REGISTRO
=============================================*/
function validarRegistro(){

    var cedula = document.querySelector("#cedulaHistoria").value;


/* VALIDAR CEDULA */

if(cedula != ""){

    var caracteres = cedula.length;
    var expresion = /^[0-9]/;

    if(caracteres > 8){

        document.querySelector("label[for='validarRegistro']").innerHTML += "<br>Escriba por favor 8 o menos caracteres.";

        return false;
    }

    if(!expresion.test(cedula)){

        document.querySelector("label[for='validarRegistro']").innerHTML += "<br>Solo puede escribir Numeros";

        return false;

    }

}

	
return true;

}
/*=====  FIN VALIDAR REGISTRO  ======*/