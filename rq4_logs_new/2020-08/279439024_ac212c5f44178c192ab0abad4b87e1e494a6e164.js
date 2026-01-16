window.onload = function () {
  let toggleBtn = document.querySelector(".page-header__toggle");
  let pageHeader = document.querySelector(".page-header");
  toggleBtn.classList.remove("page-header__toggle_close");
  pageHeader.classList.add("page-header_close");
  toggleBtn.onclick = function (e) {
    e.preventDefault();
    toggleBtn.classList.toggle("page-header__toggle_close");
    pageHeader.classList.toggle("page-header_close");
  }

  let btnBefore = document.querySelector(".btn-before-js")
  let btnAfter = document.querySelector(".btn-after-js")
  let slideBefore = document.querySelector(".slide-before-js")
  let slideAfter = document.querySelector(".slide-after-js")
  let stripe = document.querySelector(".before-after__slider-inner-stripe");
  btnBefore.onclick = function () {
    slideBefore.classList.toggle("before-after__slide_active");
    slideAfter.classList.toggle("before-after__slide_active");
    btnBefore.classList.toggle("before-after__toggle_active");
    btnAfter.classList.toggle("before-after__toggle_active");
    stripe.classList.toggle("before-after__slider-inner-stripe_active")
  }
  btnAfter.onclick = function () {
    slideBefore.classList.toggle("before-after__slide_active");
    slideAfter.classList.toggle("before-after__slide_active");
    btnBefore.classList.toggle("before-after__toggle_active");
    btnAfter.classList.toggle("before-after__toggle_active");
    stripe.classList.toggle("before-after__slider-inner-stripe_active")
  }



}