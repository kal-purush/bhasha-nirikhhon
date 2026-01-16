/* creamos la variables de recuperar la contraseña */
function validarForgot(){
    var mail, expresion;
    mail = document.getElementById("mail").value;
    expresion = /\w+@\w+\.+[a-z]/;

    /* obligatoriedad del tipo de email que debe ingresar para recuperar la contraseña */
    if(!expresion.test(mail)){
        alert("Debe ingresar un correo electrónico valido");
        return false;
    }
}