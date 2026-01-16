(function(window) {
    var timeoutId,
        html = window.document.documentElement;
    function setFontSize() {
        var htmlWidth = html.getBoundingClientRect().width;
        var ratio = htmlWidth / 640;
        if(htmlWidth < 640) {
            window.rem = ratio * 0.625;
        } else {
            window.rem = 0.625;
        }
        window.responseRatio = ratio;
        html.style.fontSize = window.rem + "rem";
    }
    window.addEventListener("resize", function() {
        clearTimeout(timeoutId), timeoutId = setTimeout(setFontSize, 300);
    }, false);
    setFontSize();
})(window);