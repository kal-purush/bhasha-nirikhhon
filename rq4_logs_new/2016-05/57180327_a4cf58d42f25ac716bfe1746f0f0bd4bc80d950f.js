/**
 * Created by CP on 16/4/30.
 */

//save as PNG
var canvas = document.querySelector("#canvas");

document.querySelector("#save").addEventListener("click", function () {
    html2canvas(document.querySelector("#visualization"), {canvas: canvas}).then(function (canvas) {
        console.log('Drew on the existing canvas');
		//drawSchedule();
        Canvas2Image.saveAsImage(canvas, 490, 850, "png");
		//alert("finish");
    });
}, false);