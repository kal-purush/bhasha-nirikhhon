let menuIcon = document.querySelector(".top-header .row .menu .icon .bars");
let closeMenuIcon = document.querySelector(".top-header .row .menu .icon .xmark");
let menuNavList = document.querySelector(".top-header .media-ul");

menuIcon.addEventListener("click", showMenuList);

function showMenuList() {
  menuNavList.style.cssText = "visibility: visible; opacity: 1; flex-direction: column; transition: .3s; left: 0;"
  closeMenuIcon.style.cssText = "visibility: visible; opacity: 1; transition: .3s;"
  menuIcon.style.cssText = "visibility: hidden; opacity: 0; transition: .3s;"
};

closeMenuIcon.addEventListener("click", closeMenuList);

function closeMenuList() {
  menuIcon.style.cssText = "visibility: visible; opacity: 1;"
  closeMenuIcon.style.cssText = "visibility: hidden; opacity: 0;"
  menuNavList.style.cssText = "visibility: hidden; opacity: 0; left: -100%; transition: .5s;"
};

// 

let topHeader = document.querySelector(".top-header");

window.addEventListener("scroll", () => {
  console.log(window.scrollY);
  if(window.scrollY > 70) {
    topHeader.style.cssText = `background-color: white;
    box-shadow: rgba(0, 0, 0, 0.1) 0px 4px 12px;
    `
  } else {
    topHeader.style.cssText = `background-color: transparent; box-shodow: none;`
  }
});
