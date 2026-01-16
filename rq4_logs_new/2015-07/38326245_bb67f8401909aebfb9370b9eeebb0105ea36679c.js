function calcularPoder(tipo, idTotal) {
	var valorPoder = 0;
	$.each($("input[tipo='" + tipo + "']"), function(key, value) {
		if ($.isNumeric(value.value) && value.value > 0) {
			value.className = value.className.replace(/(?:^|\s)error-validacion(?!\S)/g, '');
			valorPoder = valorPoder + (value.attributes[0].value.replace(".", "") * value.attributes[1].value * value.value);
		} else if (value.value != "") {
			value.focus();
			if (value.className.indexOf("error-validacion") < 0) {
				value.className += " error-validacion";
			}
			return false;
		}
	});

	valorPoder = valorPoder.toLocaleString();
	$("#" + idTotal + "").text(valorPoder);

	calcularTotal();
}

function calcularTotal() {
	var total = 0;
	$.each($("input[tipo != '']"), function(key, value) {
		if ($.isNumeric(value.value) && value.value > 0) {
			total = total + (value.attributes[0].value.replace(".", "") * value.attributes[1].value * value.value);
		}

	});
	total = total.toLocaleString();
	$("#valorTotal").text(total);
}

function limpiarFormulario() {
	$.each($("input[power != '']"), function(key, value) {
		value.value = "";
	});
}