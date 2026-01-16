document.addEventListener("DOMContentLoaded", () => {
  const navbar = document.querySelector(".navbar");
  const fadeIns = document.querySelectorAll(".fade-in");
  const fadeSlides = document.querySelectorAll(".fade-slide");

  // Navbar Background Change on Scroll
  window.addEventListener("scroll", () => {
    if (window.scrollY > 50) {
      navbar.classList.add("scrolled");
    } else {
      navbar.classList.remove("scrolled");
    }
  });

  // Fade-in effect when elements come into view
  const observer = new IntersectionObserver(
    (entries, observer) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("visible");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.2 }
  );

  fadeIns.forEach((el) => observer.observe(el));
  fadeSlides.forEach((el) => observer.observe(el));
});