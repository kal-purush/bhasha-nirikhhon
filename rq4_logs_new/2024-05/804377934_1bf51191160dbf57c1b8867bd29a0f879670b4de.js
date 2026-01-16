const scroll = new LocomotiveScroll({
  el: document.querySelector(".main"),
  smooth: true,
});

// const elem = document.querySelector('.elem');

// console.log(elem.getAttribute('data-image'))

let elemC = document.querySelector(".elem-container");
let fixedImg = document.querySelector(".fixed-image");

elemC.addEventListener("mouseenter", () => {
  fixedImg.style.display = "block";
});
elemC.addEventListener("mouseleave", () => {
  fixedImg.style.display = "none";
});

const elems = document.querySelectorAll(".elem");
elems.forEach(function (e) {
  e.addEventListener("mouseenter", () => {
    let img = e.getAttribute("data-image");
    fixedImg.style.backgroundImage = `url(${img})`;
  });
});

function page4Animation() {
  var swiper = new Swiper(".mySwiper", {
    slidesPerView: "auto",
    centeredSlides: false,
    spaceBetween: 0,
  });
}
function swiperAnimation() {
  var swiper = new Swiper(".mySwiper", {
    slidesPerView: "auto",
    centeredSlides: false,
    spaceBetween: 0,
  });
}
function menuAnimation() {
  let flaag = 0;
  const navImg = document.querySelector("nav img");
  const menu = document.querySelector("nav h3");
  const full = document.querySelector(".full-scr");
  menu.addEventListener("click", () => {
    // full.style.zindex=100;
    if (flaag == 0) {
      full.style.top = 0;
      navImg.style.opacity = 0;
      // menu.style.display="none";
      flaag = 1;
    } else {
      full.style.top = "-100%";
      navImg.style.opacity = 1;
      flaag = 0;
    }
  });
}
function loadingAnimation() {
  const loader = document.querySelector(".loader");
  setTimeout(() => {
    loader.style.top = "-100%";
  }, 4000);
}
loadingAnimation();
page4Animation();
swiperAnimation();
menuAnimation();

// setInterval(() => {
//   console.log(window.innerHeight);
// }, 2000);



let full = document.querySelector('.full-scr');

full.addEventListener('click',()=>{
  full.style.top="-100%";
})