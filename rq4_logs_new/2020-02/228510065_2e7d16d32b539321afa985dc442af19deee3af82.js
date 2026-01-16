window.onload = () => {
    var lsc = (a, as) => {
        if (a.length == 0) {
            return;
        }
        var x = new XMLHttpRequest();
        x.open('GET', a.shift());
        x.onload = () => {
            if (x.status === 200) {
                var el = document.createElement('script');
                el.innerHTML = x.responseText;
                document.head.appendChild(el);
            } else {
                console.error(x.status);
            }
            if (!as) {
                lsc(a, as);
            }
        };
        x.send();
        if (as) {
            lsc(a, as);
        }
    };
    var o = /*$ordered*/;
    var a = /*$async*/;
    lsc(o, false);
    lsc(a, true);
}