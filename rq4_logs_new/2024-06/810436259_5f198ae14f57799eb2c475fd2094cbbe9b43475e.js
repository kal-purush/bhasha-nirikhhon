//show coding image attribution
const codeImg = document.getElementById("codeImg");
const codeImgAttrib = document.getElementById("codeImgAttrib");

codeImg.addEventListener('mouseover', ()=>{
    codeImgAttrib.style.opacity = 1;
})

codeImg.addEventListener('mouseout', ()=>{
    codeImgAttrib.style.opacity = 0;
})

//show email image attribution
const emailImg = document.getElementById("emailImg");
const emailImgAttrib = document.getElementById("emailImgAttrib");

emailImg.addEventListener('mouseover', ()=>{
    emailImgAttrib.style.opacity = 1;
})

emailImg.addEventListener('mouseout', ()=>{
    emailImgAttrib.style.opacity = 0;
})

//change active nav button based on the url
let activeSection = document.URL.split("#")[1];

const navButtons = document.querySelectorAll(".navItem");
let active = document.querySelector(".active");

navButtons.forEach(button =>{
    if (button.innerText.toLowerCase() == activeSection) {
        active.classList.remove("active");
        active = button;
        button.classList.add("active");
        button.click()
    }
})

//change active nav button on click
navButtons.forEach(button =>{
    button.addEventListener("click", ()=>{
        active.classList.remove("active");
        active = button;
        button.classList.add("active");
    })
})