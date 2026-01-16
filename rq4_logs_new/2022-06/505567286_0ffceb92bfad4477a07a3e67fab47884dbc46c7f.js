const body = document.querySelector("body");
const slider = document.querySelector(".slider");
const waiveAnimation = document.querySelector(".wave");

slider.addEventListener("click", activateDarkMode);
waiveAnimation.addEventListener("click", waiveOnClick);

function activateDarkMode() {
  body.classList.toggle("dark-mode");
}

function waiveOnClick() {
  waiveAnimation.classList.add("wave-click");
}