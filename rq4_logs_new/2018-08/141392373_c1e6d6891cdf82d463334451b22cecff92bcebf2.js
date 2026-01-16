




$(function () {

    $("#form_evento input").keyup(function () {
        var form = $(this).parents("#form_evento");
        var check = checkCampos(form);
        if (check) {
            $("#enviar").prop("disabled", false);
        }
        else {
            $("#enviar").prop("disabled", true);
        }
    });
});

//Funci�n para comprobar los campos de texto

function checkCampos(obj) {
    var camposRellenados = false;
    obj.find("input").each(function () {
        var $this = $(this);
        if ($this.val().length > 0) {
            camposRellenados = true;
            return false;           
        }
        else {
            camposRellenados = false;
            return false;
        }
    });
    if (camposRellenados == false) {
        return false;
    }
    else {
        return true;
    }
};