let navbar = document.querySelector("span#icon-navbar");
let content = document.querySelector("nav");
let retornar = document.querySelector("div.return");

navbar.addEventListener("click", function(){
    navbar.style.display = "none";
    content.style.display = "block";
});

retornar.addEventListener("click", function(){
    navbar.style.display = "block";
    content.style.display = "none";
});