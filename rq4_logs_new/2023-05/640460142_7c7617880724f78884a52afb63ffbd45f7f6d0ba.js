function clearInputPass() {
    var passwordInput = document.getElementById("password-input");
    
    if (passwordInput.value === "Password") {
      passwordInput.value = "";
    }

}

function clearInputUser() {
    var usernameInput = document.getElementById("username-input");

    if (usernameInput.value === "Username") {
      usernameInput.value = "";
    }

}

function maskInput() {
    var passwordInput = document.getElementById("password-input");
    passwordInput.type = "password";
}