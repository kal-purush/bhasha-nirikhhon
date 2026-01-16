const navbar = document.querySelector(".navbar");

window.addEventListener("scroll", (e) => {
  navbar.classList.toggle("sticky", window.scrollY > 100);
});