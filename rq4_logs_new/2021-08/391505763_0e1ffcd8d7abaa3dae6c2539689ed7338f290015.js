<script type='text/javascript' src='script.js' charset='utf-8'></script>
function passwordCheck() {
	var pass = document.getElementById("passIn");
	alert("test")
}

function passLen(password) {
	if (password.length > 15 || password.length < 8) {
		return false;
	} else {
		return true;
	}
}

function startLetter(password) {
	if (password.charCodeAt(0) >= 65 && password.charCodeAt(0) <= 122) {
		return true;
	}
	return false;
}

function containsNumber(password) {
	for (let i = 0; i < password.length; i++) {
		if (password.charAt(i) = < 57 && password.charAt(i) >= 48) {
			return true;
		}
	}
	return false;
}

function containsSpecialCharacters(password) {
	for (let i = 0; i < password.length; i++) {
		if (password.charAt(i) >= 33 && input.charAt(i) <= 47) {
			return true;
		}
		if (password.charAt(i) >= 58 && password.charAt(i) <= 64) {
			return true;
		}
		if (password.charAt(i) >= 91 && password.charAt(i) <= 96) {
			return true;
		}
		if (password.charAt(i) >= 120 && password.charAt(i) < 126) {
			return true;
		}
	}
	return false;
}