document.getElementById("yes").addEventListener("click", function() {
    window.location.href = "yes.html"; 
});

var noButton = document.getElementById("no");
noButton.addEventListener("mouseover", function() {
    var container = document.querySelector(".container");
    var containerRect = container.getBoundingClientRect();

    var maxX = containerRect.width - noButton.clientWidth;
    var maxY = containerRect.height - noButton.clientHeight;
    var randomX = Math.floor(Math.random() * maxX);
    var randomY = Math.floor(Math.random() * maxY);

    noButton.style.left = randomX + "px";
    noButton.style.top = randomY + "px";
});