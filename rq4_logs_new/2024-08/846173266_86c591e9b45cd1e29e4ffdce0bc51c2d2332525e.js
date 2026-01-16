document.addEventListener("DOMContentLoaded", function () {
  const moreInfoBtn = document.getElementById("more-info-btn");
  const moreInfo = document.getElementById("more-info");
  const visitorMessage = document.getElementById("visitor-message");
  const submitMessage = document.getElementById("submit-message");
  const messageDisplay = document.getElementById("message-display");

  moreInfoBtn.addEventListener("click", function () {
    if (moreInfo.style.display === "none") {
      moreInfo.style.display = "block";
      moreInfoBtn.textContent = "Less About Me";
    } else {
      moreInfo.style.display = "none";
      moreInfoBtn.textContent = "More About Me";
    }
  });

  submitMessage.addEventListener("click", function () {
    const message = visitorMessage.value.trim();
    if (message) {
      messageDisplay.textContent = `Thank you for your message: "${message}"`;
      visitorMessage.value = "";
    } else {
      messageDisplay.textContent = "Please enter a message before submitting.";
    }
  });
});