/* 
 * Autor: Thelaw
 */


function recupera_cuenta(email) {
    let parametros = {
        "email": email
    };
    $.ajax({
        data: parametros, //datos que se envian a traves de ajax
        url: '../controlador/RecuperaClave.php', //archivo que recibe la peticion
        type: 'post', //método de envio
        beforeSend: function () {
            $("#resultado").html('<center><img  src="../img/loading/carga.svg" alt="carga" /></center>');
            //$('#contenido').html('<div class="loading" align="center"><img src="../img/loading/12.svg" alt="loading" /><br/>Un momento, por favor...</div>');
            //$('#contenido').html('<div class="loading" align="center"><br/>Un momento, por favor...</div>');
        },
        success: function (response) { //una vez que el archivo recibe el request lo procesa y lo devuelve
            $("#resultado").html(response);
        }
    });
}

function registrar_usuario(nombre,apellido,direccion,email,nick,clave) {
    let parametros = {
        "nombre": nombre,
        "apellido": apellido,
        "direccion": direccion,
        "email": email,
        "nick": nick,
        "clave": clave
        
    };
    $.ajax({
        data: parametros, //datos que se envian a traves de ajax
        url: '../controlador/Registrar.php', //archivo que recibe la peticion
        type: 'post', //método de envio
        beforeSend: function () {
            $("#resultado").html('<center><img  src="../img/loading/carga.svg" alt="carga" /></center>');
            //$('#contenido').html('<div class="loading" align="center"><img src="../img/loading/12.svg" alt="loading" /><br/>Un momento, por favor...</div>');
            //$('#contenido').html('<div class="loading" align="center"><br/>Un momento, por favor...</div>');
        },
        success: function (response) { //una vez que el archivo recibe el request lo procesa y lo devuelve
            $("#resultado").html(response);
            $('#nombre').val('');
                $('#apellido').val('');
                $('#direccion').val('');
                $('#email').val(''); 
                $('#nick').val('');
                $('#clave').val('');

                
        }
    });
}

//function ingreso(nick,clave) {
//    let parametros = {
//        "nick": nick,
//        "clave": clave
//    };
//    $.ajax({
//        data: parametros, //datos que se envian a traves de ajax
//        url: '../controlador/Login.php', //archivo que recibe la peticion
//        type: 'post', //método de envio
//        beforeSend: function () {
//            $("#resultado").html('<center><img  src="../img/loading/carga.svg" alt="carga" /></center>');
//            //$('#contenido').html('<div class="loading" align="center"><img src="../img/loading/12.svg" alt="loading" /><br/>Un momento, por favor...</div>');
//            //$('#contenido').html('<div class="loading" align="center"><br/>Un momento, por favor...</div>');
//        },
//        success: function (response) { //una vez que el archivo recibe el request lo procesa y lo devuelve
////            $("#resultado").html(response);
////            $('#nick').val('');
////            $('#clave').val('');
//  
//
//                
//        }
//    });
//}