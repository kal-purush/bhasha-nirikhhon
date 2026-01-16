$(document).ready(function() {
    let fecha = new Date();
    let añoActual = fecha.getFullYear();
    añoActual = añoActual - 2000;
    // Define la función para formatear el texto
    function formatearTexto(texto, intervalo, longitudMinima) {
        // Completa con ceros a la derecha si el largo es menor a `longitudMinima`
        while (texto.length < longitudMinima) {
            texto += '0';
        }
        
        // Inserta un espacio cada `intervalo` caracteres
        let resultado = '';
        for (let i = 0; i < texto.length; i++) {
            resultado += texto[i];
            if ((i + 1) % intervalo === 0 && i !== texto.length - 1) {
                resultado += ' ';
            }
        }

        return resultado;
    }

    // Escucha el evento de entrada en el campo de texto
    $("#cardnumber").on('input', function() {
        let textoIngresado = $(this).val();
        let textoFormateado = formatearTexto(textoIngresado, 4, 16); // Espacio cada 4 caracteres y mínimo 15 de longitud
        $("#numbers").text(textoFormateado); // Muestra el resultado en el DOM
    });

    $("#cardname").on('input', function(){
        let nombreIngresado = $(this).val();
        $("#name").text(nombreIngresado);
    });

    $("#month").on('input', function() {
        let textoIngresado = $(this).val();
        let textoFormateado = formatearTexto(textoIngresado, 4, 2); // Espacio cada 4 caracteres y mínimo 15 de longitud
        $("#monthlabel").text(textoFormateado); // Muestra el resultado en el DOM
    });

    $("#year2").on('input', function() {
        let textoIngresado = $(this).val();
        let textoFormateado = formatearTexto(textoIngresado, 4, 2); // Espacio cada 4 caracteres y mínimo 15 de longitud
        $("#yearlabel").text(textoFormateado); // Muestra el resultado en el DOM
    });

    $("#cvc").on('input', function() {
        let textoIngresado = $(this).val();
        let textoFormateado = formatearTexto(textoIngresado, 4, 3); // Espacio cada 4 caracteres y mínimo 15 de longitud
        $("#cvclabel").text(textoFormateado); // Muestra el resultado en el DOM
    });

    $("#buttonconfirm").click(function(){
        let contador = 0;
        let nombreTarjeta = $("#cardname").val();
        let numerosTarjeta = $("#cardnumber").val();
        let mesTarjeta = $("#month").val();
        let expTarjeta = $("#year2").val();
        let cvc = $("#cvc").val();
        if( (/^[a-zA-Z]+$/.test(nombreTarjeta))){
            contador++;
        }else{
            $("#error1").css("display", "block");
            $("#cardname").val("");
        }
        if( (numerosTarjeta.trim() != "") && (/^\d+$/.test(numerosTarjeta)) && (numerosTarjeta.length == 16)){
            contador++;
        }else{
            $("#error2").css("display", "block");
            $("#cardnumber").val("");
        }
        if( (mesTarjeta.trim() != "") && (mesTarjeta > 0) && (mesTarjeta <= 12) && (/^\d+$/.test(mesTarjeta)) && (mesTarjeta.length == 2) ){
            contador++;
        }else{
            $("#error3").css("display", "block");
            $("#month").val("");
        }
        if( (expTarjeta.trim() != "") && (expTarjeta <= (añoActual+5)) && (expTarjeta >= (añoActual-5))  && (/^\d+$/.test(expTarjeta)) && (expTarjeta.length == 2)){
            contador++;
        }else{
            $("#error4").css("display", "block");
            $("#year2").val("");
        }
        if( (cvc.trim() != "") && (cvc.length == 3)){
            contador++;
        }else{
            $("#error5").css("display", "block");
            $("#cvc").val("");
        }
        if(contador == 5){
            $("#cardinfo").css("display", "none");
            $("#finish").css("display", "flex");
        }
    });
    
    $("#buttoncontinue").click(function(){
        $("#cardinfo").css("display", "block");
        $("#finish").css("display", "none");
        $("#cardname").val("");
        $("#cardnumber").val("");
        $("#month").val("");
        $("#year2").val("");
        $("#cvc").val("");
        $("#cvclabel").text("000");
        $("#numbers").text("0000 0000 0000 0000");
        $("#name").text("Jane Appleseed");
        $("#monthlabel").text("00");
        $("#yearlabel").text("00");
    });
});