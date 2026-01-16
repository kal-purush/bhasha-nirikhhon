var hamburger = document.getElementById("hamburger");
var links = document.getElementById("links");

hamburger.addEventListener("click", function() {
    if (links.style.display === "flex")
    {links.style.display = "none";}
    else {links.style.display = "flex", 
    links.style.flexDirection = "column";}
});