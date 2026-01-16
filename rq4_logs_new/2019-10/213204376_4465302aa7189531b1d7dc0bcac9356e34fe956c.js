function inicioPag(){
  var tipoInicio= Cookies.get('btn-registro');

  if(tipoInicio=="registro"){
    $(".circulo-1").css("color","grey");
    $(".circulo-2").css("color","green");

      $(".contenedor-login-registro").animate(
        {left: "-100%"}
        , 100, function() {

          $("#cambioLoginRegistro").val("Iniciar Sesion");
        });
        Cookies.remove('btn-registro');
  }
  

  $("#cambioLoginRegistro").click(cambioLoginRegistro);

  function cambioLoginRegistro(){
  pos= $(".contenedor-login-registro").css("left");

    // $("#cambioLoginRegistro").click(function () {
      var valueBoton = $("#cambioLoginRegistro").val();

      if(valueBoton=="Registrarse"){
        $(".contenedor-login-registro").animate(
          {left: "-100%"}
          , 600, function() {
            cambioColorCirculos();
            $("#cambioLoginRegistro").val("Iniciar Sesion");
          });

            Cookies.set('btn-registro','registro');
        }else{
          $(".contenedor-login-registro").animate(
            {left: "0%"}
            , 600, function() {
  cambioColorCirculos();
              $("#cambioLoginRegistro").val("Registrarse");

            });
            Cookies.set('btn-registro','inicio');
        }
    // });
  }

  function cambioColorCirculos(){
  var circle1 = $(".circulo-1").css("color");
    var cont=0,pos,comp;

  // setTimeout(function(){
  while(cont<1){
    pos= $(".contenedor-login-registro").css("left");
    comp= $(".article-login").css("width");
  // alert("POS: "+pos +"COMP: "+ comp)
    if(pos=="0px"||pos == ("-"+comp) ){
      if (circle1=="rgb(0, 128, 0)"){
        $(".circulo-1").css("color","grey");
        $(".circulo-2").css("color","green");
      }
      else{
        $(".circulo-2").css("color","grey");
        $(".circulo-1").css("color","green");
      }
      cont++;
    }
  }
  // },1000);

    }

}


document.addEventListener("DOMContentLoaded",inicioPag);