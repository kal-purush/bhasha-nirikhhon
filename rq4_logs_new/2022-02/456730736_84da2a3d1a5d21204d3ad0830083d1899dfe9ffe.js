$(document).ready(function (){
    $(function(){
        $('#iFechaNac').on('change', calcularEdad);
    })
    function calcularEdad() {
        
        fecha = $(this).val();
        var hoy = new Date();
        var cumpleanos = new Date(fecha);
        var edad = hoy.getFullYear() - cumpleanos.getFullYear();
        var m = hoy.getMonth() - cumpleanos.getMonth();

        if (m < 0 || (m === 0 && hoy.getDate() < cumpleanos.getDate())){
            edad--;
        }
        $('#iAnios').val(edad);
    }
})

$(document).ready(function (){
  $(function(){
    $(".numero").on("change", validarNumeros);
  })
    function validarNumeros(){
      if (isNaN($("#iNros").val()) || isNaN($("#iCodPost").val())){
      alert("Debe ingresar sólo números");
      $("#iNros").val("");
      $("#iCodPost").val("");
      return true;
      }
    }
})

$(document).ready(function (){
  $(function(){
    $("#iDni").on("change", validarDni);
  })
    function validarDni(){
      if($("#iDni").val().length != 8 || isNaN($("#iDni").val())){
      alert("Documento: Debe ingresar 8 números");
      $("#iDni").val("");

      return true;
      }
    }
})
$(document).ready(function (){
  $(function(){
    $("#iTelefono").on("change", validarTelefono);
  })
    function validarTelefono(){
      if ($("#iTelefono").val().length != 10 || isNaN($("#iTelefono").val())) {
        alert("Teléfono: Debe ingresar 10 números, ejemplo 2302999888");
        $("#iTelefono").val("");
        return true;
        }
    }
})


$(document).ready(function (){
  $(function(){
    $("#iPais").on("keyup", habilitarProvincia);
  })
  function habilitarProvincia(){
    if($("#iPais").val()!="" || $("#iPais").val().length!=0){
      $("#iPcia").prop('disabled', false);
    return true;
    }
  }
})

$(document).ready(function (){
  $(function(){
    $("#iPcia").on("keyup", habilitarCiudad);
  })
  function habilitarCiudad(){
    if($("#iPcia").val()!="" && $("#iPcia").val()!=""){
      $("#iCity").prop('disabled', false);
      return true;
    }
  }
})

$(document).ready(function (){
    $(function(){
      $("#botonEnviar").on('click', validarEnvio);
    })
      function validarEnvio(){
        var nombre= $("#iNombre").val();
        var apellido=$("#iApellido").val();
        var documento=$("#iDni").val();
        var correo=$("#iEmail").val();  
        
        if (nombre!="" 
          && apellido!=""
          && documento!="" 
          && correo!=""){
          return true;
            }else{
           alert("Debe completar todos los campos marcados con (*)");
            return false;
            }
      }
})