document.onreadystatechange = () => {
  if (document.readyState === "complete") {
    // document ready
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
      anchor.addEventListener("click", function(e) {
        e.preventDefault();

        document.querySelector(this.getAttribute("href")).scrollIntoView({
          behavior: "smooth",
          block: "start"
        });
      });
    });
  }
};