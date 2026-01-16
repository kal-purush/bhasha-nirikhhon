window.onload = () => {
    var img = new Image();
    img.src = "cat-618470_640.jpg";
    img.onload = main.bind(null, img);
};

function add(x, y) {
    return [x[0] + y[0], x[1] + y[1]];
}

function mul(x, y) {
    return [x[0] * y[0] - x[1] * y[1], x[0] * y[1] + x[1] * y[0]];
}

function norm2(x) {
    return x[0] * x[0] + x[1] * x[1];
}

function conj(x) {
    return [x[0], -x[1]];
}

function scale(x, c) {
    return [c * x[0], c * x[1]];
}

function inv(x) {
    return scale(conj(x), 1 / norm2(x));
}

function div(x, y) {
    return mul(x, inv(y));
}

function main(img) {
    var canvas1 = <HTMLCanvasElement>document.getElementById("canvas1");
    var canvas2 = <HTMLCanvasElement>document.getElementById("canvas2");
    var w = 500, h = 500;
    var sw = w / 2, sh = h / 2;
    var imgCanvas = imgToCanvas(img, sw, sh);
    var src = imgCanvas.getContext("2d").getImageData(0, 0, sw, sh);
    draw(canvas1, src, w, h, sw, sh, false);
    draw(canvas2, src, w, h, sw, sh, true);
}

function draw(canvas, src, w, h, sw, sh, transform) {
    canvas.width = w, canvas.height = h;
    var ctx = canvas.getContext("2d");
    var dest = ctx.createImageData(w, h);
    var alpha = [100, 0];
    var beta = [60, 60];
    for (var y = 0; y < h; y++) {
        for (var x = 0; x < w; x++) {
            var i = (y * w + x) * 4;
            var xx = x - w / 2, yy = y - h / 2;
            var pp = xx, qq = yy;
            if (transform) {
                xx *= 10/w;
                yy *= 10/h;
                var z = div(add(scale(alpha, -1), mul([xx, yy], beta)), [xx - 1, yy]);
                pp = z[0], qq = z[1];
            }
            var p = pp + sw / 2, q = qq + sh / 2;
            if (0 <= p && p < sw && 0 <= q && q < sh) {
                var j = (Math.floor(q) * sw + Math.floor(p)) * 4;
                dest.data[i] = src.data[j];
                dest.data[i + 1] = src.data[j + 1];
                dest.data[i + 2] = src.data[j + 2];
                dest.data[i + 3] = src.data[j + 3];
            }
        }
    }
    ctx.putImageData(dest, 0, 0);
    ctx.translate(w / 2, h / 2);
    ctx.beginPath();
    ctx.moveTo(-w / 2, 0);
    ctx.lineTo(w / 2, 0);
    ctx.moveTo(0, -w / 2);
    ctx.lineTo(0, w / 2);
    ctx.stroke();
}

function getPixel(canvas: HTMLCanvasElement, x, y) {
    return canvas.getContext("2d").getImageData(x, y, 1, 1);
}

function imgToCanvas(img: HTMLImageElement, sw, sh) {
    var canvas = document.createElement("canvas");
    canvas.width = sw;
    canvas.height = sh;
    var ctx = canvas.getContext("2d");
    ctx.drawImage(img, 0, 0, sw, sh);
    return canvas;
}