(function() {
    var tencent = 10;
    var t = new Date().getSeconds();
    tencent = t;
    var timer = setInterval(function() {
        tencent--;
        console.log(tencent);
        if (tencent == 1) {
            clearInterval(timer);
        }
        document.querySelector('h3').innerHTML = tencent;
    }, 1000)
})()