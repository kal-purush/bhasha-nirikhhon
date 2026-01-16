const bugregBtn = document.querySelector('.burger');
const burgerMenu = document.querySelector('.burger__menu');

bugregBtn.addEventListener('click', function()  {
   burgerMenu.classList.toggle('burger__menu--active');
   bugregBtn.classList.toggle('burger-active');
})