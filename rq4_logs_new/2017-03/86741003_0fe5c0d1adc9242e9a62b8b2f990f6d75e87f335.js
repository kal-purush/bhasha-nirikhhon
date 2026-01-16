/*global document, select*/
var img = document.getElementsByClassName("portfolio"),
    i;

select.onchange = function () {
    "use strict";
    if (select.value === "original") {
        for (i = 0; i < img.length; i += 1) {
            img[i].classList.remove("black-white");
            img[i].classList.remove("invert");
            img[i].classList.remove("sepia");
        }
    } else if (select.value === "black-white") {
        for (i = 0; i < img.length; i += 1) {
            img[i].setAttribute("class", "portfolio black-white");
        }
    } else if (select.value === "sepia") {
        for (i = 0; i < img.length; i += 1) {
            img[i].setAttribute("class", "portfolio sepia");
        }
    } else if (select.value === "invert") {
        for (i = 0; i < img.length; i += 1) {
            img[i].setAttribute("class", "portfolio invert");
        }
    }
};