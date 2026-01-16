const navBar = document.getElementById("nav_bar");
const cl = document.getElementById("click")
let scrollValue;
let a = addEventListener("scroll",()=>{
    scrollValue = window.scrollY;
    if(scrollValue>50){
        navBar.style.opacity = 0.6;
    } else {
        navBar.style.opacity = 1; 
    }
})
navBar.addEventListener("mouseenter",()=>{
    navBar.style.opacity = 1;
})
navBar.addEventListener("mouseleave",()=>{
    if(scrollValue>50){
        navBar.style.opacity = 0.6 
    }
})