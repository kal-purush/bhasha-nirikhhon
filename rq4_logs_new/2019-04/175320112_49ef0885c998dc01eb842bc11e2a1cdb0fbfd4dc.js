window.editarCategoria=function(categoria){

	categoria=JSON.parse(categoria);

    for (var propiedad in categoria){

      $(".edit_categoria #"+propiedad).val(categoria[propiedad])
      $(".edit_categoria #"+propiedad+' option[value="'+categoria[propiedad]+'"]').attr('checked', true);
    }

    if (propiedad=="sexo") {
        $(".edit_evento #sexo").val(evento.sexo)
        $(".edit_evento #"+propiedad+' option[value="'+evento.sexo+'"]').attr('checked', true);
      }

  $(".editar_modal").click();
}


window.crearCategoria=function(){


var DatCre={};
  $(".crear_categoria input, .crear_categoria select, .crear_categoria textarea").map(function(key, val){

    $(".crear_categoria #"+val.id).removeClass("is-invalid");
    $("small").remove();


    DatCre[val.id]=val.value;
    if(val.id=="inscripcion_habilitada" || val.id=="auto_email" || val.id=="auto_numeracion"){
      if($('#'+val.id).prop('checked')==true){
        DatCre[val.id]=1;
      } else{ DatCre[val.id]=0; }
    }
  });

  $(".crear_part_btn").hide("fast", function(){
    $(".crear_eve_lo").show("fast");
  });
  console.log(DatCre);
  $.ajax({
      type: 'post',
      url: url+"/categorias",
      data: DatCre,
      success: function(result){
      	console.log(result);
        $(".crear_eve_lo").hide("fast", function(){
          $(".crear_part_btn").show("fast");
        });
        if (result!="Exito") {
          for (var key in result){
            $(".crear_categoria #"+key).addClass("is-invalid");
            $(".crear_categoria #"+key).after('<small id="us_error err_part" style="color:red;">'+result[key]+'</small>');
           }
        } else{
          $("#home").empty();
          $("#home").append('<div id="vista_gategorias"></div>');
          $("#vista_gategorias").load("categoriras_act");

          $(".crear_categoria input, .crear_categoria select").map(function(key, val){
            $(".crear_categoria #"+val.id).val("");
          })

          swal("Listo!", "El evento ha sido registrado exitosamente", "success")
        }
       }
  });
}

window.actualizarCategoria=function(){


  $("small").remove()
  var Data={}, val=0;
  $(".edit_categoria input, .edit_categoria select, .edit_categoria textarea").map(function(key, value){
    $(".edit_categoria #"+value.id).removeClass("is-invalid");
    if (value.value=="") {
      $(".edit_categoria #"+value.id).addClass("is-invalid");
      $(".edit_categoria #"+value.id).after('<small id="us_error err_part" style="color:red;">Este campo es obligatorio</small>');
      val++;
    }
    
    Data[value.id] = value.value;

  })

  if (Number(Data.edad_minima)>=Number(Data.edad_maxima)) {
      $(".edit_categoria #edad_maxima").addClass("is-invalid");
      $(".edit_categoria #edad_maxima").after('<small id="us_error err_part" style="color:red;">La edad minima debe ser menor que la edad maxima</small>');

      $(".edit_categoria #edad_minima").addClass("is-invalid");
      $(".edit_categoria #edad_minima").after('<small id="us_error err_part" style="color:red;">La edad minima debe ser menor que la edad maxima</small>');
      val++;
    }


  if (val==0) {
    updateCategoria(Data);
  }


}



window.updateCategoria=function(Datos){

  $(".upd_participante_btn").hide("fast", function(){
      $(".cate_lo").show("fast");
  });

  Datos._method="post";
  $.ajax({
      type: 'PUT',
      url: url+"/categorias/"+Datos.id,
      data: Datos,
      success: function(result){
        $(".cate_lo").hide("fast", function(){
            $(".upd_participante_btn").show("fast");
        });
        if (!result[0].id) {
          for (var key in result){
            $(".edit_categoria #"+key).addClass("is-invalid");
            $(".edit_categoria #"+key).after('<small id="us_error err_part" style="color:red;">'+result[key]+'</small>');
           }
        } else {
          $("#info_cat_"+result[0].id).empty();

          for (var key in result[0]){

            if (key!="id_usuario" && key!="sexo") {
              $("#info_cat_"+result[0].id).append("<td>"+result[0][key]+"</td>");
            }

              if (key=="sexo") {
                if (result[0][key]==1) {
                   $("#info_cat_"+result[0].id).append("<td>Femenino</td>");
                } else{
                  $("#info_cat_"+result[0].id).append("<td>Masculino</td>");
                }
              }
          }
          var info=JSON.stringify(result[0]);
          info=info.replace(/"/g, "&quot;");
          $("#info_cat_"+result[0].id).append('<td style="display: flex;" id="btn_eve">'+
                '<button onclick="editarevento('+"'"+info+"'"+')" style="padding: .25rem .5rem;" type="button" class="mb-2 btn btn-primary mr-2"><i style="font-size: 25px" class="material-icons">border_color</i></button>'+
                '<button onclick="editarParticipante()" style="padding: .25rem .5rem;" type="button" class="mb-2 btn btn-danger mr-2"><i style="font-size: 25px" class="material-icons">delete</i></button>'+
              '</td>')

          swal("Listo!", "Categoria actualizada exitosamente", "success");
        }
      }
  });
}



window.eliminarCategoria=function(id){

  
    swal({
          title: "Espera!",
          text: "¿Estas seguro de eliminar esta categoria? Los participantes asignados a este categoria quedaran sin categoria asignada, al igual que el evento asociado",
          icon: "warning",
          buttons: true,
          dangerMode: true,
        })
        .then((willDelete) => {
          if (willDelete) {
            swal("Listo!", "Categoria eliminado exitosamente", "success")
            $("#info_cat_"+id).hide("slow");
            $("#info_cat_"+id).remove();
                  $.ajax({
                  type: 'DELETE',
                  url: url+"/categorias/"+id,
                  success: function(result){
                    console.log(result)
                  }
              });

            }
        });


}