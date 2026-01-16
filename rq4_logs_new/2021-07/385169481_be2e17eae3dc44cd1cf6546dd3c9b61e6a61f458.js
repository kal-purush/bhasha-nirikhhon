$(document).ready(() => {
  var loginForm = $(".login-form");
  loginForm.submit((e) => {
    e.preventDefault();
    var userName = $(".username").val();
    var password = $(".password").val();
    if (!userName || !password) {
      alert("Please fill all the credentials");
    } else if (userName !== password) {
      alert("Invalid credentails");
    } else if (userName === password) {
      localStorage.setItem("status", true);
      alert("Login Successful");
      window.location.href = "/order.html";
    } else {
      alert("Something went wrong");
    }
  });
});