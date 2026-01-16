// vars

let body = document.getElementsByTagName("body")[0];
let button = document.getElementById("color-scheme-selector");
let icon = document.getElementById("favicon")

// color scheme selector script

button.addEventListener("click", function() {
    if (body.getAttribute("color-scheme") == "dark") {
        body.setAttribute("color-scheme", "light");
        icon.href = "icon-light.png";
    }else {
        body.setAttribute("color-scheme", "dark");
        icon.href = "icon-dark.png";
    }
})
