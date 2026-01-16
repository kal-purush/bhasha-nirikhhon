var formErrMsg = 'Please fill the form first.';
var defaultInpuMap = new Map();

function checkEmailInput(email) {
	var re = /\S+@\S+\.\S+/;
	var result = re.test(email);
	if (!result) {
		formErrMsg = 'You have entered an invalid email address.';
		$('#contact-form-email').css({
			borderBottom : '1px solid #2a6c73'
		});
		return false;
	}
	$('#contact-form-email').css({
		borderBottom : '1px solid rgba(0,0,0,.12)'
	});
	return true;
}

function checkEmptyInput(input, field) {
	var id = '#contact-form-' + field;
	var defaultText = defaultInpuMap.get(id);
	if (input == '' || input == defaultText) { // invalid input
		formErrMsg = 'Please provide a valid ' + field;
		$(id).css({
			borderBottom : '1px solid #2a6c73'
		});
		return false;
	}
	$(id).css({
		borderBottom : '1px solid rgba(0,0,0,.12)'
	});
	return true;
}

function checkAllInputs() {
	return checkEmailInput($('#contact-form-email').val())
			&& checkEmptyInput($('#contact-form-name').val(), 'name')
			&& checkEmptyInput($('#contact-form-subject').val(), 'subject')
			&& checkEmptyInput($('#contact-form-message').val(), 'message');
}

function setDefaultBackIfNeeded(inputElement) {
	var id = inputElement.id;
	var defaultText = defaultInpuMap.get(id);
	var itsLabel = $("label[for='" + id + "']");
	var newText = $('#' + id).val();
	
	if(newText == '') {
		itsLabel.text(defaultText);
	} 
}

function removeDefaultLabel(inputElement) {
	var id = inputElement.id;
	var itsLabel = $("label[for='" + id + "']");
	var oldText = itsLabel.html();
	
	if(oldText != '') {
		defaultInpuMap.set(id, oldText)
	}
	
	itsLabel.text('');
}

$(document).ready(function() {
	// remove labels when typing
	$('input').on('click touchstart', function(e) {
		e.stopPropagation();
		removeDefaultLabel(this);
	});
	$('textarea').on('click touchstart', function(e) {
		e.stopPropagation();
		removeDefaultLabel(this);
	});
	
	$('input').blur(function() {
		setDefaultBackIfNeeded(this);
	});
	
	$('textarea').blur(function() {
		setDefaultBackIfNeeded(this);
	});
	
	// click hidden form submit
	$('#contatct-form-button').on('click touch', function(e) {
		e.stopPropagation();
		$('#contatct-form-upload').click();
	});
	
	// submit contact form
	$('#contatct-form').on('submit', function(e) {
		e.preventDefault();
		if (checkAllInputs()) {
			// get form data
			var formData = {
				name : $('#contact-form-name').val(),
				email : $('#contact-form-email').val(),
				subject : $('#contact-form-subject').val(),
				message : $('#contact-form-message').val()
			};

			$.ajax({
				url : '/message',
				type : 'POST',
				contentType : "application/json",
				dataType : 'json',
				data : JSON.stringify(formData),
//				beforeSend : showSpinner()
			}).done(function() {
				// hide spinner
				$('#modalSpinner').css({
					visibility : 'hidden'
				});

				// show message
				var modal = document.getElementById('contactSentMsg');
				modal.style.visibility = "visible";

				$('.close').on('click touchstart',  function(e) {
					e.stopPropagation();
					modal.style.visibility = "hidden";
				});

				$(document).on('click touchstart', function(e) {
					e.stopPropagation();
					if (e.target == modal) {
						modal.style.visibility = "hidden";
					}
				});

			}).always(function() {
				// always
			}).fail(function(message) {
				showError("Failed to send message. Please Try again.");
				// hide spinner
				$('#modalSpinner').css({
					visibility : 'hidden'
				});
			});
		} else {
			// showError
		}
	});
});