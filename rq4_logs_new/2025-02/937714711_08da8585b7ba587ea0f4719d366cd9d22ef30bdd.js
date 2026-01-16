function toggleMenu() {
  const menu = document.querySelector(".menu-links");
  const icon = document.querySelector(".hamburger-icon");
  menu.classList.toggle("open");
  icon.classList.toggle("open");
}
// Show/Hide Arrow Button on Scroll
window.onscroll = function() {
  const arrowUpBtn = document.getElementById("arrowUpBtn");
  if (document.body.scrollTop > 200 || document.documentElement.scrollTop > 200) {
    arrowUpBtn.style.display = "block"; // Show button
  } else {
    arrowUpBtn.style.display = "none"; // Hide button
  }
};

// Scroll to Top Function
function scrollToTop() {
  window.scrollTo({ top: 0, behavior: "smooth" });
}