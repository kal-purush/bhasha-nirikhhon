


function fechaHoyMenu() {



    var f = new Date();
    var fecha = f.getFullYear() + "-" + (f.getMonth() + 1) + "-" + f.getDate();




    return fecha;
}
salir = function () {

    debugger;
    $.ajax({
        type: "POST",
        url: baseurl + "index.php/auth/logout",
        dataType: 'json',
        data: {'<?php echo $this->security->get_csrf_token_name(); ?>': '<?php echo $this->security->get_csrf_hash(); ?>'},
        success: function (res) {
            window.location.href = '';

        },
        error: function (request, status, error) {
          
//            window.location.href = '';
            console.log(error.message);
            //direccionar al login?


        }
    });


}
getUsuario = function () {
    $.ajax({
        type: "POST",
        url: baseurl + "index.php/auth/getUser",
        dataType: 'json',
        data: {'<?php echo $this->security->get_csrf_token_name(); ?>': '<?php echo $this->security->get_csrf_hash(); ?>'},
        success: function (res) {



            $('#nombreEmp1').append(res.NombreCompleto);

            $('#detalleEmp').append(res.NombreCompleto + 'Administrador' +
                    '<small>Administrador del Sistema</small>');
            $('#nombreEmp2').append(res.NombreCompleto);
//            getUsuarioimg(res.id);



        },
        error: function (request, status, error) {
            debugger;
            console.log(error.message);
            //direccionar al login?


        }
    });


}
getUsuarioimg = function (idEmpleado) {
    $.ajax({
        type: "POST",
        url: baseurl + "index.php/empleado/get_empleadoById/" + idEmpleado,
        dataType: 'json',
        data: {'<?php echo $this->security->get_csrf_token_name(); ?>': '<?php echo $this->security->get_csrf_hash(); ?>'},
        success: function (res) {


            $('#empimg').attr('src', './assets/imagenes/empleado/' + res.response.emp_imagen);
            $('#empimg1').attr('src', './assets/imagenes/empleado/' + res.response.emp_imagen);
            $('#empimg2').attr('src', './assets/imagenes/empleado/' + res.response.emp_imagen);



        },
        error: function (request, status, error) {


            console.log(error.message);



        }
    });


};
getPedidosCant = function (fechamenu) {


    $.ajax({
        type: "POST",
        url: baseurl + "index.php/pedido/get_pedidosFechaPed/" + fechamenu,
        dataType: 'json',
        data: {'<?php echo $this->security->get_csrf_token_name(); ?>': '<?php echo $this->security->get_csrf_hash(); ?>'},
        success: function (res) {
            $('#cantPedidos').empty();
            $('#cantPedidostext').empty();

            $('#cantPedidos').append(parseInt(res));
            $('#cantPedidostext').append('Tienes ' + parseInt(res) + ' Pedidos Para Enviar');

        },
        error: function (request, status, error) {

            console.log(error.message);
            //direccionar al login?


        }
    });


}
getPedidosCantEn = function (fechamenu) {


    $.ajax({
        type: "POST",
        url: baseurl + "index.php/pedido/get_pedidosFechaEnv/" + fechamenu,
        dataType: 'json',
        data: {'<?php echo $this->security->get_csrf_token_name(); ?>': '<?php echo $this->security->get_csrf_hash(); ?>'},
        success: function (res) {

            $('#cantPedidosF').empty();
            $('#cantPedidosFtext').empty();

            $('#cantPedidosF').append(parseInt(res));
            $('#cantPedidosFtext').append('A Finalizado ' + parseInt(res) + ' Pedidos');

        },
        error: function (request, status, error) {

            console.log(error.message);
            //direccionar al login?


        }
    });


}
var fechamenu = fechaHoyMenu();
getPedidosCant(fechamenu);
getPedidosCantEn(fechamenu);
getUsuario();

var int2 = self.setInterval("refreshmenu()", 30000);
function refreshmenu()
{
    var fechamenu = fechaHoyMenu();
    getPedidosCant(fechamenu);
    getPedidosCantEn(fechamenu);
}




  