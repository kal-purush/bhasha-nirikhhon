var height = window.innerHeight;
var width = window.innerWidth;

var example = <HTMLCanvasElement>document.getElementById("example");
example.width  = width;
example.height = height;
var ctx = example.getContext('2d');

ctx.fillRect(0, 0, example.width, example.height);

Array.prototype.diff = function(a) {
    return this.filter(function(i) {return a.indexOf(i) < 0;});
};

function drawPointRadNdeg(rad, deg) {
    ctx.fillRect(width/2 + Math.cos(deg) * rad, height/2 + Math.sin(deg) * rad, 1, 1);
}

function getOnRadDeg(rad, deg) {
    return [
        width / 2 + Math.cos(deg) * rad,
        height / 2 + Math.sin(deg) * rad
    ];
}

function drawPoint(vec) {
    ctx.fillRect(vec[0], vec[1], 1, 1);
}

function drawLine(vec1, vec2, col?) {
    if (col) {
        ctx.strokeStyle = col;
    }
    ctx.beginPath();
    ctx.moveTo(vec1[0], vec1[1]);
    ctx.lineTo(vec2[0], vec2[1]);
    ctx.stroke();
}

function rgb(r,g,b) {
    return 'rgb(' + r + ',' + g + ',' + b + ')';
}

function grey(col) {
    return rgb(col, col, col);
}

function getRadius(a, b) {
    return Math.sqrt(Math.pow(a[0] - b[0], 2) + Math.pow(a[1] - b[1], 2));
}
function checkRad(a, b, rad) {
    return Math.sqrt(Math.pow(a[0] - b[0], 2) + Math.pow(a[1] - b[1], 2)) < rad;
}

function sortRadius(c) {
    return (a, b) => {
        return getRadius(a, c) > getRadius(b, c) ? 1 : -1;
    }
}

function getCheck(b, rad) {
    return function(a) {
        return checkRad(a, b, rad);
    }
}

function generateMirrorMap(size_) {
    var size = size_ / 2;
    var list = [];
    for (var i = 0; i < size; i++) {
        var xRnd = getRandom(0, width / 2 - 20);
        var yRnd = getRandom(0, height);
        list.push([
            xRnd,
            yRnd
        ], [
            width - xRnd,
            yRnd
        ]);
    }
    return list;
}

function getRandom(min, max) {
    return Math.round(min - 0.5 + Math.random() * (max - min + 1));
}

function getRandomA(min, max) {
    return min - 0.5 + Math.random() * (max - min + 1);
}

function clear() {
    ctx.fillStyle = '#000';
    ctx.fillRect(0, 0, width, height);
}

function fill(col) {
    ctx.fillStyle = col;
    ctx.fillRect(0, 0, width, height);
}

function map(x, in_min, in_max, out_min, out_max) {
    return Math.floor((x - in_min) * (out_max - out_min) / (in_max - in_min) + out_min);
}

function generate() {
    var circle = [],
        deg = 0;

    while (deg < Math.PI*2) {
        circle.push({
            deg: deg,
            radNoize: 0//getRandom(0, 1) ? -50 : 50
        });
        deg+=getRandomA(0.8, 1);
    }
    return circle;
}

function createGradient(a, b) {
    var grad = ctx.createLinearGradient(a[0], a[1], b[0], b[1]);
    grad.addColorStop(0, "black");
    grad.addColorStop(1, "white");
    return grad;
}

function loop() {
    var maxSizeVec = 250;
    return window.setInterval(() => {
        clear();

        var begin, back;
        ctx.fillStyle = '#fff';

        radArr = radArr.map((circle) => {
            back = false;
            for (var deg = 0; deg < Math.PI * 2; deg += 0.1) {
                begin = getOnRadDeg(circle.rad, deg);

                if (circle.rad < 500) {
                    circle.clr = map(circle.rad, 200, 500, 0, 255);
                } else {
                    circle.clr = 255;
                }

                var near = mapPoint.filter(getCheck(begin, maxSizeVec));
                near.sort(sortRadius(begin)).forEach((end, i, arr) => {
                    var rad = getRadius(begin, end);
                    var color = map(rad, 0, maxSizeVec, circle.clr, 0);
                    drawLine(begin, end, grey(color));
                });

                if (back) {
                    drawLine(begin, back, grey(circle.clr));
                }
                back = begin;
            }
            drawLine(getOnRadDeg(circle.rad, 0), back, grey(circle.clr));

            circle.rad -= 5;
            if (circle.rad < 200) {
                circle.rad = radius;
            }
            return circle;
        });

    }, 30);
}

var work = true;
var mapPoint = generateMirrorMap(100);
var radius = width / 2 + 250;

var radArr = [];
for (var i = 0; i < 3; i++) {
    radArr.push({
        rad: radius + 300 * i,
        clr: 255
    });
}

var id = loop();

window.addEventListener('click', () => {
    if (work) {
        window.clearInterval(id);
    } else {
        id = loop();
    }
    work = !work;
});