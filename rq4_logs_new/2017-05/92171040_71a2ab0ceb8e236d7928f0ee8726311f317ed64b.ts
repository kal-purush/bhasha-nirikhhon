// var canvas: HTMLCanvasElement;
// var ctx: CanvasRenderingContext2D;


// function gameLoop() {
//    requestAnimationFrame(gameLoop);
//    ctx.fillStyle = "black";
//    ctx.fillRect(0, 0, 1280, 720);
//    ctx.beginPath();
//    ctx.strokeStyle = "red";
//    ctx.lineWidth = 5;
//    ctx.arc(400, 400, 100, 0, 2 * Math.PI);
//    ctx.stroke();
// }

// window.onload = () => {
//    canvas = <HTMLCanvasElement>document.getElementById('cnvs');
//    ctx = canvas.getContext("2d");
//    gameLoop();
// }

var canvas = document.querySelector('canvas');


canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

//creeren van 2d objecten in de Canvas
var c = canvas.getContext('2d');



// aanmaak van de variabele van de muis, waarbij de coordinaten een waarden krijgen in de eventListener 'mousemove'
var mouse = {
    x: undefined,
    y: undefined
}

var maxRadius = 40;
// var minRadius = 3;

//kleur waardes van de kleur array die ervoor zorgt welke kleur elke cirkeltje krijgt.
var colorArray = [
    '#BD2320',
    '#CDCDCD',
    '#244036',
    '#797072',
    '#7B3F1E',

];


// event listener voor de mouse move, waarbij de coordinaten worden uitgeprint in de console.
window.addEventListener('mousemove', function(event){
    mouse.x = event.x;
    mouse.y = event.y;
    console.log(mouse);   

})

// als het venster van grootte word aangepast.
window.addEventListener('resize', function(){
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;

    init();
})

//cirkel word hier gedefineerd met de this waardes

function Circle(x, y, dx, dy, radius) {
    this.x = x;
    this.y = y;
    this.dx = dx;
    this.dy = dy;
    this.radius = radius;
    this.minRadius = radius;
    this.color = colorArray[Math.floor(Math.random() * colorArray.length)];


// hier worden de objecten getekend in de venster.
    this.draw = function() {
        c.beginPath();
        c.arc(this.x ,this.y , this.radius, 0, Math.PI * 2, false);
        c.fillStyle = this.color;
        c.fill();
    }

    this.update = function() {
            if( this.x + this.radius > innerWidth || 
                this.x - this.radius < 0) {
                this.dx= -this.dx;
            }

            if (this.y + this.radius > innerHeight || 
                this.y - this.radius < 0) {
                this.dy = -this.dy;
            }
            
            this.x += this.dx;
            this.y += this.dy;


            // interactivity waarbij de muis hover ervoor zorgt dat de cirkeltjes groter en kleiner worden.

            if  (mouse.x - this.x < 50 && mouse.x - this.x > -50 &&
                 mouse.y - this.y < 50 && mouse.y - this.y > -50
                ) {
                    if (this.radius < maxRadius) {
                        this.radius += 1;
                    }
                
                }   else if (this.radius > this.minRadius) {
                        this.radius -= 1;
                }
            
                this.draw();
            }


}

// maakt een array aan van de cirkeltjes die getekend worden.
var circleArray = [];

for (var i = 0; i < 2000; i++){
    var x = Math.random() * (innerWidth - radius * 2) + radius;
    var y = Math.random() * (innerHeight - radius * 2) + radius;
    var dx = (Math.random() - 0.5);
    var dy = (Math.random() - 0.5);
    var radius = Math.random() * 3 + 1;
    circleArray.push(new Circle(x ,y ,dx ,dy , radius));

}

// creert cirkeltjes opnieuw als je je browser venster resized.
var circleArray = [];

function init(){
    
    circleArray = [];

    for (var i = 0; i < 5000; i++){
        var x = Math.random() * (innerWidth - radius * 2) + radius;
        var y = Math.random() * (innerHeight - radius * 2) + radius;
        var dx = (Math.random() - 0.5);
        var dy = (Math.random() - 0.5);
        var radius = Math.random() * 3 + 1;
        circleArray.push(new Circle(x ,y ,dx ,dy , radius));
    }
}

function animate(){
    requestAnimationFrame(animate);
    c.clearRect(0, 0, innerWidth, innerHeight
        );
    for (var i = 0; i < circleArray.length; i++) {

        circleArray[i].update();
    }
}

init();
animate();

console.log('test');   