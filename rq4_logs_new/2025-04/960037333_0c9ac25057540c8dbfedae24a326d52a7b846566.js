document.addEventListener("DOMContentLoaded", () => {
    const carousel = document.getElementById("carousel");
  
    // Clone images for looping effect
    carousel.innerHTML += carousel.innerHTML;
  
    let scrollAmount = 0;
    const scrollSpeed = 1;
  
    function autoScrollCarousel() {
      scrollAmount += scrollSpeed;
      carousel.scrollLeft = scrollAmount;
  
      if (scrollAmount >= carousel.scrollWidth / 2) {
        scrollAmount = 0; // Reset to start of original content
      }
    }
  
    setInterval(autoScrollCarousel, 20); // Adjust speed as needed
  });
  
  