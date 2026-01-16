const menuActive = document.querySelector('.menu-active');
const overlayActive = document.querySelector('.dark-overlay');
const burger = document.querySelector('.burger-menu__btn');
const menuClose = document.querySelector('.menu-close');

function toggleMenu() {
    menuActive.classList.toggle('hidden');
    overlayActive.classList.toggle('hidden');

}

burger.addEventListener('click', toggleMenu);
menuClose.addEventListener('click', toggleMenu);