var canvas = document.querySelector("canvas");
var context = canvas.getContext("2d");
var button = document.querySelector("button");
var spriteSheetURL = "demos/DVD/icon.png";
var image = new Image();
image.src = spriteSheetURL;
var DVD;
let speed = 0.25;
let logoSpeed = 20;
//speed = 1;

image.onload = function () {
    canvas.width = document.documentElement.clientWidth*0.8 || 200;
    canvas.height = document.documentElement.clientHeight*0.8 || 200;
    Start();
    DoFrame();
    setInterval(DoFrame, 10);
}
button.onclick = function () {
    Start();
}

function Start(){
    DVD = { x: Math.random()*(canvas.width-256),
            y: Math.random()*(canvas.height-128),
            dx: Math.random()*logoSpeed,
            dy: 0,
    };
    DVD.dy = (logoSpeed-DVD.dx)-logoSpeed/2;
    DVD.dx = DVD.dx-logoSpeed/2;
}
function DoFrame() {
    canvas.width = document.documentElement.clientWidth*0.8 || 200;
    canvas.height = document.documentElement.clientHeight*0.8 || 200;
    console.log(canvas.width, canvas.height);
    context.drawImage(
        image,
        0, -2,
        256, 126,
        DVD.x, DVD.y,
        256, 128,
    );
    DVD.x += DVD.dx*speed;
    DVD.y += DVD.dy*speed;
    if (DVD.x + 256 > canvas.width || DVD.x < 0) {
        DVD.dx = -DVD.dx;
    }
    if (DVD.y + 128 > canvas.height || DVD.y < 0) {
        DVD.dy = -DVD.dy;
    }
}