// GSAP animation for the about us section
document.addEventListener("DOMContentLoaded", function () {
    gsap.from("h1", { 
      duration: 1, 
      y: -50, 
      opacity: 0, 
      ease: "power3.out" 
    });
  
    gsap.to("p", {
      duration: 1.5,
      opacity: 1,
      stagger: 0.3,
      ease: "power3.out",
      delay: 0.5
    });
  });
  