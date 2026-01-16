'use strict';

var submit = document.querySelector('#submit');
var firstName = document.querySelector('#fname');
var middleName = document.querySelector('#mname');
var lastName = document.querySelector('#lname');
var firstNameShip = document.querySelector('#fname-ship');
var middleNameShip = document.querySelector('#mname-ship');
var lastNameShip = document.querySelector('#lname-ship');

var copyNameChkb = document.getElementById('name-ship-chkbx');
var copyAddChkb = document.querySelector('#bill-add-match-ship');

if (copyNameChkb) {
	console.log('copy name checkbox exists');
}

//event handlers
copyNameChkb.onclick = function () {
	if (copyNameChkb.checked) {
		console.log('copy name checkbox val: ' + copyNameChkb.value);
		firstNameShip.value = firstName.value;
		middleNameShip.value = middleName.value;
		lastNameShip.value = lastName.value;
	} else {
		console.log('copy name checkbox val: ' + copyNameChkb.value);
		firstNameShip.value = '';
		middleNameShip.value = '';
		lastNameShip.value = '';
	}
};

submit.onclick = function () {
	var validationErrors = '';
	var email1 = document.querySelector('#email1');
	var email2 = document.querySelector('#email2');
	var emailValidationError = '';

	//validate zip code

	//validate email address match
	if (email1.value !== email2.value) {
		emailValidationError += 'Email addresses do not match. ';
	}

	//validate email format
	if (!email1.value.includes('@')) {
		emailValidationError += 'Email should include "@". ';
	}

	console.log('emailValidationError: ' + emailValidationError);
	email1.setCustomValidity(emailValidationError);
};

//https://www.owasp.org/index.php/Input_Validation_Cheat_Sheet