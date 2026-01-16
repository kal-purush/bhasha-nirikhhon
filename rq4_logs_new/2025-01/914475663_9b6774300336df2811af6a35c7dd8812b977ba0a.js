const log_in_btn = document.querySelector("#log-in-btn");
const sign_up_btn = document.querySelector("#sign-up-btn");
const container = document.querySelector(".container");

sign_up_btn.addEventListener("click", () => {
  container.classList.add("sign-up-mode");
});

log_in_btn.addEventListener("click", () => {
  container.classList.remove("sign-up-mode");
});


//subscribe

function subscribe() {
  const emailInput = document.getElementById("emailholder");
  const email = emailInput.value.trim();
  const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

  // Check if the email is not empty and matches both the HTML5 and custom pattern
  if (email === "") {
    alert("Please enter a valid email address!");
  } else if (!emailInput.checkValidity() || !emailPattern.test(email)) {
    alert("Please enter a valid email address!");
  } else {
    alert("You have successfully subscribed to our newsletter!");
  }
}
