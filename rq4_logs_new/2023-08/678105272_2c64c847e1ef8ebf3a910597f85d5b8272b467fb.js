document.addEventListener("DOMContentLoaded", function () {
  // Get references to the elements you want to animate
  const title = document.querySelector(".title");
  const intro = document.querySelector(".content p");

  // Add a class to initiate the animation
  title.classList.add("animate-title");
  intro.classList.add("animate-intro");
});