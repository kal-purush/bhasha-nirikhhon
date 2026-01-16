import checkPassword from "./scripts/checkPassword.js";
import submitForm from "./scripts/submitForm.js";

const inputs = Array.from(document.querySelectorAll("input[type='password']"));
const matchAlert = document.querySelector("#match_alert");
const submitButton = document.querySelector("button[type='submit']");


inputs.forEach(input => {
    input.addEventListener("input", () => {
       if (!checkPassword(inputs)) {
            inputs.forEach(input => {
                input.style.border = "red 1px solid";
                matchAlert.style.display = "block";
            });
       } else {
           inputs.forEach(input => {
               input.style.border = "green 1px solid";
               matchAlert.style.display = "none";
           });
       }
    });
});

submitButton.addEventListener("click", (e) => submitForm(e, checkPassword(inputs)));