$(document).ready(function() {
	var lugarGuardado = $("#donde").text(localStorage.getItem("lugar"));
	var llegadaGuardado = $("#llegada").text(localStorage.getItem("llegada"));
	var salidaGuardado = $("#salida").text(localStorage.getItem("salida"));
	var personasGuardado = $("#cantidad").text(localStorage.getItem("personas"));

	$("#llegadaDos").append(llegadaGuardado);
	$("#salidaDos").append(salidaGuardado);
	
});