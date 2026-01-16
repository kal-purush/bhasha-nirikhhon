document.addEventListener("DOMContentLoaded", function () {
  var showLoginLink = document.getElementById("show-login");
  var showRegisterLink = document.getElementById("show-register");
  var registrationForm = document.getElementById("registration-form");
  var loginForm = document.getElementById("login-form");

  showLoginLink.addEventListener("click", function (event) {
    event.preventDefault();
    registrationForm.classList.remove("slide-in");
    loginForm.classList.remove("slide-in");
    registrationForm.style.display = "none";
    loginForm.style.display = "block";

    loginForm.classList.add("slide-in");
  });

  showRegisterLink.addEventListener("click", function (event) {
    event.preventDefault();
    loginForm.classList.remove("slide-in");
    registrationForm.classList.remove("slide-in");
    loginForm.style.display = "none";
    registrationForm.style.display = "block";

    registrationForm.classList.add("slide-in");
  });
});