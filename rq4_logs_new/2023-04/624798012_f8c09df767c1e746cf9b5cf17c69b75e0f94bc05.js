// Sign In
var modal = document.getElementById("SignIn_Modal");
var btn = document.getElementById("signin_btn");
var close = document.getElementsByClassName("signin_close")[0];

btn.onclick = function() {
  modal.style.display = "block";
  console.log("Button clicked")
}

// When the user clicks on <span> (x), close the modal
close.onclick = function() {
  modal.style.display = "none";
}

// When the user clicks anywhere outside of the modal, close it
window.onclick = function(event) {
  if (event.target == modal) {
    modal.style.display = "none";
  }
}