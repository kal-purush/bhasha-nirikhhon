import Swal from "sweetalert2";

export function dispararSweetAlertBasico(title, text, icon, textBoton){
    Swal.fire({
        title: title,
        text: text,
        icon: icon,
        confirmButtonText: textBoton
        });
}