//Set date
var today = function(){
	var date = new Date();

	var dayOfMonth = date.getDate();
	var dayOfWeekStr = "Sunday";
	var monthNameStr = "December";
	var year = date.getFullYear();

	switch (date.getDay()) {
		case 0:
			dayOfWeekStr = "Domingo"
			break;
		case 1:
			dayOfWeekStr = "Lunes";
			break;
		case 2:
			dayOfWeekStr = "Martes";
			break;
		case 3:
			dayOfWeekStr = "Miércoles";
			break;
		case 4:
			dayOfWeekStr = "Jueves"
			break;
		case 5:
			dayOfWeekStr = "Viernes";
			break;
		case 6:
			dayOfWeekStr = "Sábado";
			break;
		default:
			dayOfWeekStr = "Sábado";
			break;
	}

	switch (date.getMonth()) {
		case 0:
			monthNameStr = "Enero";
			break;
		case 1:
			monthNameStr = "Febrero";
			break;
		case 2:
			monthNameStr = "Marzo";
			break;
		case 3:
			monthNameStr = "Abril";
			break;
		case 4:
			monthNameStr = "Mayo";
			break;
		case 5:
			monthNameStr = "Junio";
			break;
		case 6:
			monthNameStr = "Julio";
			break;
		case 7:
			monthNameStr = "Agosto";
			break;
		case 8:
			monthNameStr = "Septiembre";
			break;
		case 9:
			monthNameStr = "Octubre";
			break;
		case 10:
			monthNameStr = "Noviembre";
			break;
		case 11:
			monthNameStr = "Diciembre";
			break;
	}

	return dayOfWeekStr + ", " + dayOfMonth + " de " + monthNameStr + " " + year;
}