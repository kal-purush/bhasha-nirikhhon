// Html'den elemanları çekd
const navbar = document.querySelector(".navbar");
const menuBtn = document.querySelector("#menu-btn");

// Menu btn'e tıklanınca navbar kısmına class ekle çıkar

menuBtn.addEventListener("click", () => {
  // Navbar kısmına class ekle-çıkar
  navbar.classList.toggle("active");
});