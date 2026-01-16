var tablaDelivery;

function init() {
    ListarDeliverys();
    IniciarCampos();
    ListarDepartamento();
}

function IniciarCampos() {


      $("#DeliveryPrecio").change(function () {
        var ingreso_bonificacion = $("#DeliveryPrecio").val();
        if (ingreso_bonificacion != '') {
            $("#DeliveryPrecioO").val(parseFloat(ingreso_bonificacion));
            $("#DeliveryPrecio").val("S/. " + Formato_Moneda(parseFloat(ingreso_bonificacion), 2));
        }
        else {
            $("#DeliveryPrecioO").val(0);
            $("#DeliveryPrecio").val("S/. 0.00");
        }
    });

    $("#DeliveryPrecio").click(function () {
        $("#DeliveryPrecio").val($("#DeliveryPrecioO").val());
    });

    $("#DeliveryPrecio").blur(function () {
        $("#DeliveryPrecio").val("S/. " + Formato_Moneda($("#DeliveryPrecioO").val(), 2));
    });


    $("#FormularioDelivery").on("submit", function (e) {
        RegistroDelivery(e);
    });

}

function ListarDeliverys() {
    tablaDelivery = $('#tablaDelivery').dataTable({
        "aProcessing": true
        , "aServerSide": true
        , "processing": true
        , "paging": false, // Paginacion en tabla
        "ordering": true, // Ordenamiento en columna de tabla
        "info": true, // Informacion de cabecera tabla
        "responsive": true, // Accion de responsive
        "lengthMenu": [[10, 25, 50, -1], [10, 25, 50, "All"]]
        , "order": [[2, "asc"]]
        , "bDestroy": true
        , "columnDefs": [
            {
                "className": "text-center"
                , "targets": [2]
            }
            , {
                "className": "text-left"
                , "targets": [0]
            }, {
                "className": "text-right"
                , "targets": []
            }, {
                "className": "text-wrap"
                , "targets": [1]
            }
         , ]
        , "ajax": { //Solicitud Ajax Servidor
            url: '/Mantenimiento/Delivery/ListarDelivery'
            , type: "POST"

            , dataType: "JSON"
            , error: function (e) {
                console.log(e.responseText);
            }
        }, // cambiar el lenguaje de datatable
        oLanguage: español
    , }).DataTable();
}


function RegistroDelivery(event) {

    var DeliveryDelivery=$("#DeliveryPrecioO").val();
    event.preventDefault();
    var formData = new FormData($("#FormularioDelivery")[0]);

    formData.append("DeliveryDelivery",DeliveryDelivery);

    console.log(formData);
    $.ajax({
        url: "/Mantenimiento/Delivery/InsertUpdateDelivery",
        type: "POST",
        data: formData,
        contentType: false,
        processData: false,
        success: function (data, status) {
            data = JSON.parse(data);
            console.log(data);
            var Mensaje = data.Mensaje;
            var Error = data.Error;
            if (Error) {
                mensaje_warning(Mensaje);
            } else {
                mensaje_success(Mensaje);
                LimpiarDelivery();
                $("#ModalDelivery").modal("hide");
                tablaDelivery.ajax.reload();
            }
        },
        error: function (e) {
            console.log(e.responseText);
        },
        complete: function () {}
    });
}

/*_____________Listado Ubigeo________________*/


function ListarDepartamento() {
    $.post("/Mantenimiento/Producto/ListarDepartamento", function (ts) {
        $("#DeliveryDepartamento").empty();
        $("#DeliveryDepartamento").append(ts);
    });
    $("#DeliveryDepartamento").change(function () {
        var idDepartamento = $(this).val();
        ListarProvincia(idDepartamento);
        $("#DeliveryDistrito").empty();
        $("#DeliveryDistrito").html("<option value='0'>--- SELECCIONE ---</option>")
    });
}

function ListarProvincia(idDepartamento) {
    $.post("/Mantenimiento/Producto/ListarProvincia", {
        idDepartamento: idDepartamento
    }, function (ts) {
        $("#DeliveryProvincia").empty();
        $("#DeliveryProvincia").append(ts);
    });
    $("#DeliveryProvincia").change(function () {
        var idProvincia = $(this).val();
        ListarDistrito(idProvincia);
    });
}

function ListarDistrito(idProvincia) {
    $.post("/Mantenimiento/Producto/ListarDistrito", {
        idProvincia: idProvincia
    }, function (ts) {
        $("#DeliveryDistrito").empty();
        $("#DeliveryDistrito").append(ts);
    });
}


function NuevoDelivery() {
    $("#ModalDelivery").modal({
        backdrop: 'static',
        keyboard: false
    });
    LimpiarDelivery();
    $("#ModalDelivery").modal("show");
    $("#tituloModalDelivery").empty();
    $("#tituloModalDelivery").append("Registro de Delivery");

}
function LimpiarDelivery() {
    $('#FormularioDelivery')[0].reset();
    $("#idDelivery").val("");
    $("#DeliveryPrecioO").val(0);
}
function Cancelar() {
    LimpiarDelivery();
    $("#ModalDelivery").modal("hide");
}

function EliminarDelivery(idDelivery, ubicacion) {
    swal({
        title: "Eliminar Delivery?",
        text: "Esta seguro de eliminar al Delivery: " + ubicacion,
        type: "warning",
        showCancelButton: true,
        confirmButtonColor: "#DD6B55",
        confirmButtonText: "Eliminar!",
        closeOnConfirm: false
    }, function () {
        $.post("/Mantenimiento/Delivery/EliminarDelivery", {
            'idDelivery': idDelivery,
            'Descripcion':ubicacion,
        }, function (data, e) {
            data = JSON.parse(data);
            if (data.Error) {
                swal("Error", data.Mensaje, "error");
            } else {
                swal("Eliminado!", data.Mensaje, "success");
                tablaDelivery.ajax.reload();
            }
        });
    });
}

init();