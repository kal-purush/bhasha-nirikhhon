var animateButton = function (e) {
    var node = e.target;
    if (e.target.parentNode.nodeName === "DIV") {
        node = node.parentNode.parentNode;
    } else {
        if (e.target.parentNode.nodeName === "A") {
            node = node.parentNode;
        }
    }
    if (!node.classList.contains("animate")) {
        node.classList.remove('animate');
        node.classList.add('animate');
        setTimeout(function () {
            node.classList.remove('animate');
        }, 400);
    }
};
var bubblyButtons = document.querySelectorAll(".bubbly-button, .button__icon, .button__text");
for (var j = 0; j < bubblyButtons.length; j++) {
    bubblyButtons[j].addEventListener('click', animateButton);
}