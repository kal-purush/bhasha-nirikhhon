function validateEmail(email) {
  var re = /\S+@\S+\.\S+/;
  return re.test(email);
}
const form = document.getElementById("form");
form.addEventListener("submit", (e) => {
  e.preventDefault();
  const email = document.getElementById("inputEmail");
  const error = document.getElementById("error");
  if (!validateEmail(email.value)) {
    email.style.marginBottom = "35px";
    email.style.border = "1px solid hsl(0, 93%, 68%)";
    error.style.opacity = "1";
  } else {
    location.reload();
  }
});