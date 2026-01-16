var idclientemod = 0;

$("#reg_zon").submit(function (e) {
    e.preventDefault();
    var params = {};

    params = formToJson(this);
    if (validar(params) != true) {
        return;
    }

    params.idcliente = idclientemod;
    salvar(params)
});



// se encarga de que los formularios no queden vacios y return true o false
function validar(params) {

    if (params.idnombre == ""){
        show_no("Alerta","El nombre de la zona no puede estar vacio");
        return false;
    }
    return true
}

function salvar(params) {
    $.ajax({
        url: window.location+"/salvar",
        data:params,
        type: 'POST',
        success: function (response) {
            var json = JSON.parse(response);

            if (json.key){
                show_yes("Relizado",json.msj);
                idclientemod = 0;
                // ActualizarTabla();
                $("#reg_zon").trigger('reset');
                location.reload();
            }
            else{
                show_no("Error",json.msj);
            }
        }
    });

}

function editar(idcliente) {

    show_loading_msg("Espere",1000);

    $.ajax({
        url: window.location+"/buscarclientebyid",
        data:{idcliente:idcliente},
        type: 'POST',
        success: function (response) {
            var json = eval(response);

            idclientemod = json.id;

            $("#idnombre").val(json.nombre);

        }
    });
}

function confirmarEliminar(idcliente) {
    show_confirm("Aviso!","Desea eliminar este registro",Elimiar,idcliente);
}

function Elimiar(idcliente) {
    $.ajax({
        url: window.location+"/eliminar",
        data:{idcliente:idcliente},
        type: 'POST',
        success: function (response) {
            var json = JSON.parse(response);

            if (json.key){
                show_yes("Relizado",json.msj);
                location.reload();
            }
            else{
                show_no("Error",json.msj);
            }
        }
    });
}