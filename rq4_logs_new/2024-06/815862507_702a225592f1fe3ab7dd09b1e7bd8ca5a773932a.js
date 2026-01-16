document.querySelectorAll(".btn-edit").forEach((btn) => {
    btn.addEventListener("click", (e) => {
        e.preventDefault();
        var id = btn.getAttribute("data-id");
        var name=btn.getAttribute("data-nombre");
        let id_oculto = document.getElementById("id_empleado");
        let nombre=document.getElementById("actuNombre");

        let apellido=document.getElementById("actuapellido");
        var lastname=btn.getAttribute("data-apellido");
        
        var correo=document.getElementById("actucorreo");
        let email=btn.getAttribute("data-correo");
        var password=btn.getAttribute("data-contrasena");
        let contrasena=document.getElementById("actucontrasena");


        nombre.value=name;
        id_oculto.value = id;
        apellido.value=lastname;   
        correo.value=email;
        contrasena.value=password;
        //console.log(nombre.value);
        //console.log(id_oculto);
    });
})