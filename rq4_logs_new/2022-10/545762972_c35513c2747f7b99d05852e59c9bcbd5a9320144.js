// USO USUARIO ADMIN = "Francisco"
// Y CONTRASEÑA = "Alexelcapo"

let usuario;
let contrasena;
let usuario_intentos = 0;
let contrasena_intentos = 0;
while(usuario != "Francisco"){
    usuario = prompt("Ingrese el usuario ADMIN.")
    if (usuario != "Francisco"){
        console.log("Disculpe, el usuario admin no es:", usuario)
        usuario_intentos = usuario_intentos + 1;
    }
    else if(usuario == "Francisco"){
        while(contrasena != "Alexelcapo"){
            contrasena = prompt("Ingrese la contraseña")
            if(contrasena != "Alexelcapo"){
                console.log("Disculpe, la contraseña no es", contrasena);
                contrasena_intentos = contrasena_intentos + 1;
            }
        }
    }
    }
    if(usuario_intentos >= 1){
        console.log("Tengo miedo admin, pusieron el usuario erroneo", usuario_intentos, "veces")
        if(contrasena_intentos <= 0){
            console.log("Por suerte, ingresaron la contraseña correctamente!")
        }
        else if(contrasena_intentos >= 1){
            console.log("Y encima, ingresaron mal la contraseña", contrasena_intentos,"veces")
        }
    }
    else if( usuario_intentos <= 0){
        console.log("Bienvenido, admin!")
        if(contrasena_intentos >= 1){
            console.log("Aunque sos medio tonto para ingresar mal la contraseña", contrasena_intentos, "veces..")
        }
    }