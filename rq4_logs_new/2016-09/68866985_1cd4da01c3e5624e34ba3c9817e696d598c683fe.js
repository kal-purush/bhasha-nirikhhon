$(document).ready ( function () {
	st = new Sitio()
})

function Sitio() {

	this.calendario = $('#calendario');

	this.Init();
}
Sitio.prototype.Init = function () {
	

	this.calendario.fullCalendar({
    	weekends: false,
    	locale: 'es',
    	dayClick: this.Click_Dia,
	});	

}
Sitio.prototype.Click_Dia = function(date, event, view) {

	anio = date.format("YYYY");
	mes = date.format("M");
	dia = date.format("D");

	// var url = '/reservaciones/nueva/'+anio+'/'+mes+'/'+dia+'/'

	// alert(url);

	// window.location.href = url
}