var imgmovement = document.getElementsByClassName("image");
document.onmousemove = function () {
    var x = event.clientX * 100 / window.innerWidth + "%";
    var y = event.clientY * 100 / window.innerHeight + "%";

    for (var i = 0; i < 2; i++) {
        imgmovement[i].style.left = x;
        imgmovement[i].style.top = y;
        imgmovement[i].style.transform = "translate(-" + x + ", - " + y + ")";
    }
}