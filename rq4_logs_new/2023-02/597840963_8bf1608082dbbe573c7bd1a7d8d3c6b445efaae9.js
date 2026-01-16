function show() {
    const img1 = document.querySelector("#menu-open-img");
    img1.style.display = "none";
    const img2 = document.querySelector("#menu-close-img");
    img2.style.display = "block";
    const element =  document.querySelector(".mnavbar-list-ul");
    element.style.display="block";
}
function hide() {
    const img1 = document.querySelector("#menu-open-img");
    img1.style.display = "block";
    const img2 = document.querySelector("#menu-close-img");
    img2.style.display = "none";
    const element =  document.querySelector(".mnavbar-list-ul");
    element.style.display="none";
}

   