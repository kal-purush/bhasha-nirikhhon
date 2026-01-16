/**
 * Created by guoxiaoman on 2016/10/17.
 */
//解决1像素


(function (doc, win) {
    var radio = win.devicePixelRatio || 1,
        scale = 1 / radio,
        docEl = doc.documentElement,
        meta = document.createElement("meta"),
        resizeEvt = 'orientationchange' in window ? 'orientationchange' : 'resize',
        recalc = function () {
            var clientWidth = docEl.clientWidth;
            if (!clientWidth) return;
            docEl.style.fontSize = 20 * (clientWidth / 320) + 'px';
        };
    // recalc();
    docEl.setAttribute("data-dpr", radio);
    meta.setAttribute("name", "viewport")
    meta.setAttribute("content", "width=device-width,initial-scale=" + scale + ", maximum-scale=" + scale + ", minimum-scale=" + scale + ", user-scalable=no")
    if(docEl.firstElementChild) docEl.firstElementChild.appendChild(meta);

    if  (!doc.addEventListener) return;

    win.addEventListener(resizeEvt, recalc, false);
    doc.addEventListener('DOMContentLoaded', recalc, false);
})(document, window);

//rem 计算根字体




