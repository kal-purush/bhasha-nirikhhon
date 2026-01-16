const mobileMenu = document.querySelector(".header__nav__mobile-menu");
const navList = document.querySelector(".header__nav__list");

mobileMenu.addEventListener("click", () => {
  navList.classList.toggle("header__nav__list--active");
  mobileMenu.classList.toggle("header__nav__mobile-menu--active");
});

window.addEventListener("resize", () => {
  if (window.innerWidth >= 992) {
    navList.classList.remove("header__nav__list--active");
    mobileMenu.classList.remove("header__nav__mobile-menu--active");
  }
});