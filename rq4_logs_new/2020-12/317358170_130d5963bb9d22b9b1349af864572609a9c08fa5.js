// const navShowHide = document.querySelector(".navShowHide");
// const main = document.getElementById("mainSectionContainer");
// const nav = document.getElementById("sideNavContainer");

// document.addEventListener("DOMContentLoaded", () => {
//   navShowHide.addEventListener("click", () => {
//     nav.classList.toggle("hidden");

//     main.classList.toggle("leftPadding");
//   });
// });

$(document).ready(() => {
  $(".navShowHide").on("click", () => {
    const main = $("#mainSectionContainer");
    const nav = $("#sideNavContainer");

    if (main.hasClass("leftPadding")) {
      nav.hide();
    } else {
      nav.show();
    }

    main.toggleClass("leftPadding");
  });
});