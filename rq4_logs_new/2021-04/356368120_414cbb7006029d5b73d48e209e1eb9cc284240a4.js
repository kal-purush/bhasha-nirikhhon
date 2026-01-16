const hamburger = document.querySelector("#nav-toggle");
const links = document.querySelector(".nav-links");
hamburger.addEventListener("click", () =>{
    
    if (links.classList.contains("show-links")) {
          links.classList.remove("show-links");
        } else {
          links.classList.add("show-links");
        }
})