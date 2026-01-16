var panel = document.getElementById("colorPanel");

function loopColors() {
    var i = 0;
    setInterval(
        function () {
            var val = 1 / 0.2 * (i) * 210;
            panel.style.backgroundColor = 'hsla(' + val + ', 90%, 60%, 1)';
            i = (i < 180) ? (i + 1) : 0;
        }, 1800
    );
}

loopColors();