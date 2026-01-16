// dom nodes
const openWindow = document.getElementsByClassName("open-window");
const popWindow = document.getElementsByClassName("window");
const popCloseButton = document.getElementsByClassName("close");

// console.log(openWindow);
// console.log(popWindow);
// console.log(popCloseButton);

for (let i=0; i < openWindow.length; i++) {

    popWindow[i].visibility = "hidden";

    openWindow[i].addEventListener("click", ()=>{
        popWindow[i].classList.toggle("notVisible");
    });

    popCloseButton[i].addEventListener("click", ()=>{
        popWindow[i].classList.toggle("notVisible");
    });
}

// hamburger implementation
const menuItems = document.getElementById("menu");
const hamburgerMenu = document.getElementsByClassName("hamburger");

hamburgerMenu[0].addEventListener("click", () => {
    menuItems.classList.toggle("visible");
    hamburgerMenu[0].classList.toggle("change");
})