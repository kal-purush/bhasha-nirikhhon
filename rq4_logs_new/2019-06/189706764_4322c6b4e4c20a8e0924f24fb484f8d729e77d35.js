let banner = document.querySelector(".banner");
$(document).ready(function() {
  $(".dropdown").on("show.bs.dropdown", () => {
    //Banner fade IN
    {
      banner.style.opacity = 0;
      let opacity = Number(
        window.getComputedStyle(banner).getPropertyValue("opacity")
      );
      let intervalID = setInterval(fadeIn, 100);
      function fadeIn() {
        if (opacity < 1) {
          opacity = opacity + 0.1;
          banner.style.opacity = opacity;
        } else {
          clearInterval(intervalID);
        }
      }
    }

    //Club Items
    
  });

  $(".dropdown").on("hide.bs.dropdown", () => {
    banner.style.opacity = 0; //Reseting Banner opacity
  });
});