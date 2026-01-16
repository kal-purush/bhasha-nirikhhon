import { debounce } from "../lib/communities";

(() => {
  try {
    // Values
    const showOnPx = 50;
    const backToTheTopButton = document.getElementById(
      "navigate-to-the-top-button"
    );

    // Function
    function topFunction() {
      window.scrollTo({
        top: 0,
        behavior: "smooth",
      });
    }

    function scrollFunction() {
      const scrollVal =
        document.documentElement.scrollTop || document.body.scrollTop;

      if (scrollVal > showOnPx) {
        backToTheTopButton.classList.remove("back-to-the-top-button-hidden");
      } else {
        backToTheTopButton.classList.add("back-to-the-top-button-hidden");
      }
    }

    backToTheTopButton.onclick = topFunction;

    // Desktop
    window.addEventListener("scroll", debounce(scrollFunction, 100));
    // Mobile
    window.addEventListener("touchmove", debounce(scrollFunction, 100));
  } catch (e) {
    console.log("Error, back-to-the-top: " + e);
  }
})();