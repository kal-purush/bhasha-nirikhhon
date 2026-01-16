var canvas = document.getElementById("canvas");
var c = canvas.getContext("2d");

window.addEventListener('resize', resizeCanvas, false);
function resizeCanvas() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
}
// Do it once initially to set the canvas size.
resizeCanvas();

// 1000ms = 1s
// 60 ticks per second
setInterval(tick, 1000 / 60);

var mouse = {
    x: 0, 
    y: 0,
    down: false
};

var character = {
    x: 0,
    y: 0
}

var time = 0;
var tentacles = [];
var sin, cos;
function tick(){
    sin = Math.sin(time / 30);
    cos = Math.cos(time / 30);

    c.fillStyle = "black";
    c.fillRect(0, 0, canvas.width, canvas.height);

    var mid = { x: canvas.width / 2,
                y: canvas.height / 2 };
    c.strokeStyle = "white";
    c.lineWidth = "1";

    if(mouse.down) {
        tentacles.push(new Tentacle(Math.random() * 200,
                                    Math.random(), 
                                    Math.random(), 
                                    mid,
                                    { x: Math.random() * canvas.width,
                                      y: Math.random() * canvas.height}));
    }
        
    for(i = 0; i < tentacles.length; i++) {
        tentacles[i].update(mid, mouse);
    }

    time++;
}

/*
 * Tentacle Object Constructor
 */
function Tentacle(intensity, percent1, percent2, from, to) {
    this.intensity = intensity;
    this.percent1 = percent1;
    this.percent2 = percent2;
    this.from = from;
    this.to = to;
    this.update = function() {
        this.intensity += Math.random() - 0.5;
        this.percent1 += Math.random() / 10 - 0.2;
        this.percent2 += Math.random() / 10 - 0.2;
        if(this.percent1 < 0) {
            this.percent1 = 0;
        } else if(this.percent1 > 1) {
            this.percent1 = 1;
        }
        if(this.percent2 < 0) {
            this.percent2 = 0;
        } else if(this.percent2 > 1) {
            this.percent2 = 1;
        }
        c.beginPath();
        var tangent1 = getTangentPoint(this.from, this.to, this.intensity * sin, this.percent1);
        var tangent2 = getTangentPoint(this.from, this.to, this.intensity * cos, this.percent2);
        c.moveTo(this.from.x, this.from.y);
        c.bezierCurveTo(tangent1.x, tangent1.y,
                        tangent2.x, tangent2.y,
                        this.to.x, this.to.y);
        c.stroke();
    }
}

/*
 * Returns a point that is of distance "distance", is tangent, and is
 * "percentage" up the length of the vector ("to" - "from").
 */
function getTangentPoint(from, to, distance, percentage) {
    var targetX = (to.x - from.x) * percentage + from.x; 
    var targetY = (to.y - from.y) * percentage + from.y;
    var tangent = getTangentVector(from, to, distance);
    return { x: targetX + tangent.x, y: targetY - tangent.y };
}

/*
 * Returns the tangent vector of the mouse from the center of the Canvas.
 */
function getTangentVector(from, to, length) {
    // In radians.
    var angle = getAngle(from, to);
    var tangentAngle = angle - (Math.PI / 2);
    var tanX = length * Math.cos(tangentAngle);
    var tanY = length * Math.sin(tangentAngle);
    return { x: tanX, y: tanY };
}

/*
 * Returns the angle of the vector ("to" - "from").
 */
function getAngle(from, to) {
    var vx = to.x - from.x;
    var vy = -(to.y - from.y);
    var angle = Math.atan(vy / vx);
    if(vx < 0) {
        angle = angle + Math.PI;
    } else if(vy < 0) {
        angle = angle + 2 * Math.PI;
    }
    return angle;
}

/*
 * Returns "angle" in degree form.
 */
function toDegrees(angle) {
    return angle * 360 / (2 * Math.PI);
}

/*
 * Returns the length of "vector".
 */
function getLength(vector) {
    return Math.sqrt((vector.x * vector.x) + (vector.y * vector.y));
}

//
// Listeners
//
canvas.addEventListener('mouseup', function(e) {
    mouse.down = false;
});
canvas.addEventListener('mousedown',function(e) {
    mouse.down = true;
    mouse.x = e.pageX;
    mouse.y = e.pageY;
});
canvas.addEventListener('mousemove', function(e) {
    mouse.x = e.pageX;
    mouse.y = e.pageY;
});