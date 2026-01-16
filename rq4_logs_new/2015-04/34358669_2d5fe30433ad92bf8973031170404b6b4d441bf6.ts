/// <reference path='EventTest.ts' />

window.addEventListener('load', () => {
    var et = new EventTest();

    et.addEventListener("click", function (source) {
        console.info("click 1");
        console.info(source);
    });

    et.addEventListener("select", function (source) {
        console.info("select 1");
        console.info(source);
    });

    et.addEventListener("click", function (source) {
        console.info("click 2");
        console.info(source);
    });

    et.triggerEvent("click", "HelloClick");
    et.triggerEvent("select", "HelloSelect");
    et.triggerEvent("click", "HelloClick Again");

    (<HTMLButtonElement[]> Array.prototype.slice.call(document.getElementsByTagName('button'))).forEach(function (button) {
        button.addEventListener("click", (source) => et.triggerEvent("click", source));
    });
});