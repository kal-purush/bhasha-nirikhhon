
function enviarPost(){
  var numero = 34;
  var informacion = '{"nombre":"juanma"}';
  var info = {
    "group": numero,
    "thing": informacion
  };

  $.ajax({
    type: "POST",
    //dataType: 'json',
    //async: true,
    data: JSON.stringify(info),
    contentType: "application/json; charset=utf-8",
    url: "php/ajax.php",
    success: function(response){ // ejecuta esta funcion una vez que el servidor responde.
      alert("exito");
      console.log(response);
    },
    error: function (xhr, ajaxOptions, thrownError){//tratamiento de errores
        alert(xhr.status);
        alert(thrownError);
    }
  });

}
function enviarGet(urlLink){
  /*
  var numero = 12;
  var informacion = '{"nombre":"Dardo"}';
  var info = {
    "group": numero,
    "thing": informacion
  };*/

  $.ajax({
    type: "GET",
    //dataType: 'json',
    //async: true,
    //data: JSON.stringify(info),
    contentType: "application/json; charset=utf-8",
    url: urlLink,
    success: function(response){
      //alert("exito");
      //console.log(info);
      $("#principal").html(response);//selector jquery para el div con id "principal"que le modifica el contenido html
      if(urlLink = 'principal.htm'){
        $('.pgwSlider').pgwSlider();
      }
    },
    error: function (xhr, ajaxOptions, thrownError){
        alert(xhr.status);
        alert(thrownError);
    }
  });

}