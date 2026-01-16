/**
 * alerts
 */

function setAlert(status, message) {

	var alertContent = "", alertClass;

	switch (status) {
		case "success":
			alertClass = 'alert-success';
			break;
		case "warning":
			alertClass = 'alert-warning';
			break;
		case "error":
			alertClass = 'alert-error';
			break;
		case "info":
			alertClass = 'alert-info';
			break;
		default:
			console.log('brak alertu');
	}
	
	alertContent += "<div class='alert-container'><div class='alert'>"
		+ "<a href='#' class='close' data-dismiss='alert'>&times;</a>"
		+ "<strong>"+status.toUpperCase()+ "!</strong> "+message+" </div></div>";
	
	$('#main-region').prepend(alertContent);
	$('.alert').addClass(alertClass);
	$('.alert-container').hide().fadeIn('slow').delay(3000).fadeOut('slow').queue(function() {
	    $(this).remove();
	});

}