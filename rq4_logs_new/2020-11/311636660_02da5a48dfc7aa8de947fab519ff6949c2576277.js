var navDown = document.querySelector("#toggledown");
var bring = document.querySelector("#bring");
navDown.addEventListener("click", function changeNav(){
    bring.classList.toggle("is-active");
    navDown.classList.toggle("change");
});