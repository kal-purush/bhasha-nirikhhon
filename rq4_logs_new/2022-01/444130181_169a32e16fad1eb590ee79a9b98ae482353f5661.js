const menuBtn = document.querySelectorAll(".navbar__menu-btn");
const menuOpenBtn = document.querySelector(".navbar__menu-btn--open");
const menuCloseBtn = document.querySelector(".navbar__menu-btn--close");
const navbarMenu = document.querySelector(".navbar__navigation");
const overlay = document.querySelector(".overlay");

const toggleOverlay = () => overlay.classList.toggle("overlay--active");

menuOpenBtn.addEventListener("click", (evt) => {
  toggleOverlay();
  evt.currentTarget.style.display = "none";
  menuCloseBtn.classList.toggle("navbar__menu-btn--close");
  navbarMenu.classList.toggle("navbar__navigation--active");
});

menuCloseBtn.addEventListener("click", (evt) => {
  toggleOverlay();
  evt.currentTarget.classList.toggle("navbar__menu-btn--close");
  menuOpenBtn.style.display = "block";
  navbarMenu.classList.toggle("navbar__navigation--active");
});