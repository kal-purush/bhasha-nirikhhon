document.addEventListener("DOMContentLoaded", function() {
    var el = document.getElementById("wrapper");
    var toggleBtn = document.getElementById("menu-toggle");

    toggleBtn.onclick = function() {
        el.classList.toggle("toggled");
    };
});