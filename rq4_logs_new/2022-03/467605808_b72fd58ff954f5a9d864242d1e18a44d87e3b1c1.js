var elHeader = document.querySelector(".header__inner");
var elBtn = document.querySelector(".header__menu");

elBtn.addEventListener("click", function(){
    elHeader.classList.toggle("header__inner--open")
})