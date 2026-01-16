import {
	validateEmail,
	checkLoggedInUser,
	logoutUser,
} from "../../services/utils/utils.js";

checkLoggedInUser();

const firstNameField = document.querySelector("#firstName");
const lastNameField = document.querySelector("#lastName");
const emailField = document.querySelector("#emailAddress");
const userNameField = document.querySelector("#userNameInput");
const ageField = document.querySelector("#age");
const passField = document.querySelector("#password");
const successElement = document.querySelector("#successElement");
const errorMsg = document.querySelector("#errorMsg");
const errorElement = document.querySelector("#errorElement");
const editForm = document.querySelector("#editForm");
const logoutButton = document.querySelector("#logoutButton");
logoutButton.addEventListener("click", logoutUser);

editForm.onsubmit = (event) => updateUser(event);

const updateUser = (event) => {
	// Prevent the form from reloading the page on submit.
	event.preventDefault();

	if (!validateEmail(emailField.value)) {
		errorElement.classList.remove("hidden");
		errorMsg.innerText = "Please enter a valid email address.";
		return;
	}

	if (passField.value.length < 8) {
		errorElement.classList.remove("hidden");
		errorMsg.innerText = "Password must be at least 8 characters long.";
		return;
	}

	if (userNameField.value.length < 6) {
		errorElement.classList.remove("hidden");
		errorMsg.innerText = "Username must be at least 6 characters long.";
		return;
	}

	if (firstNameField.value.length < 2) {
		errorElement.classList.remove("hidden");
		errorMsg.innerText = "First name must be at least 2 characters long.";
		return;
	}

	if (lastNameField.value.length < 2) {
		errorElement.classList.remove("hidden");
		errorMsg.innerText = "Last name must be at least 2 characters long.";
		return;
	}

	// If all the fields are valid, then hide the error element and show the success element.
	errorElement.classList.add("hidden");
	successElement.classList.remove("hidden");

	// Get the user from localStorage.
	let user = JSON.parse(localStorage.getItem(emailField.value));

	// Update the user's details.
	user = {
		firstName: firstNameField.value,
		lastName: lastNameField.value,
		email: emailField.value,
		userName: userNameField.value,
		age: ageField.value,
		password: passField.value,
	};

	// Update the user in localStorage.
	localStorage.setItem(emailField.value, JSON.stringify(user));
};