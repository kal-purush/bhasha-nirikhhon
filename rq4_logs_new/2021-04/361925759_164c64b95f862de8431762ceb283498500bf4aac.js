let canvas = document.querySelector('canvas');

canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

let c = canvas.getContext('2d');
// Rect
// c.fillStyle = "rgba(255, 0, 0, 0.5)"
// c.fillRect(100,100, 100, 100);
// c.fillStyle = "rgba(0, 0, 255, 0.5)"
// c.fillRect(200,200, 100, 100);
// c.fillStyle = "rgba(0, 255, 0, 0.5)"
// c.fillRect(300,300, 100, 100);

// console.log(canvas);
//Line
// c.beginPath();
// c.moveTo(50, 300);
// c.lineTo(300, 100);
// c.lineTo(350, 400);
// c.strokeStyle = "#fa34a3"
// c.stroke();

//Arc-Circle
// c.beginPath();
// c.arc(300, 300, 30, 0, Math.PI * 2, false);
// c.strokeStyle = "blue";
// c.stroke();

// for (let i = 0; i < 400; i++ ) {
//     let x = Math.random() * window.innerWidth;
//     let y = Math.random() * window.innerHeight;
//     let r = Math.round(Math.random() * 255);
//     let g = Math.round(Math.random() * 255);
//     let b = Math.round(Math.random() * 255);
//     let a = Math.round(Math.random());
//     c.beginPath();
//     c.arc(x, y, 30, 0, Math.PI * 2, false);
//     c.strokeStyle = `rgba(${r}, ${g}, ${b}, ${a})`;
//     c.stroke();
// }

let mouse = {
    x: undefined,
    y: undefined
}

maxRadius = 40;
minRadius = 5;

var colorArray = [
    '#9c4aff',
    '#8c43e6',
    '#7638c2',
    '#5e2c99',
    '#492378'
]

window.addEventListener('mousemove', (event) => {
    mouse.x = event.x;
    mouse.y = event.y;
})

window.addEventListener('resize', () => {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    init();
})

class Circle {
    constructor(x, y, dx, dy, r, color) {
        this.x = x;
        this.y = y;
        this.dx = dx;
        this.dy = dy;
        this.radius = r;
        this.minRadius = r;
        this.color = color;

        this.draw = () => {
            c.beginPath();
            c.arc(this.x, this.y, this.radius, 0, Math.PI * 2, false);
            c.strokeStyle = "black";
            c.fillStyle = color;
            c.fill();
            c.stroke();
        };

        this.update = () => {
            if (this.x + this.radius >= innerWidth || this.x - this.radius <= 0) {
                this.dx = -this.dx;
            }
            if (this.y + this.radius >= innerHeight || this.y - this.radius <= 0) {
                this.dy = -this.dy;
            }
            this.x += this.dx;
            this.y += this.dy;

            if (Math.abs(mouse.x - this.x) < 30 && Math.abs(mouse.y - this.y) < 30) {
                if(this.radius <= maxRadius)
                this.radius += 1;
            }
            else{
                if (this.radius >= this.minRadius)
                    this.radius -= 1;
            }

            this.draw();
        };
    }
}

let circleArray = [];

function init() {
    circleArray = []
    for (let i = 0; i < 800; i++) {
        let radius = minRadius + (Math.random() - 0.5)*minRadius;
        // let color = `rgba(${Math.round(Math.random()*255)}, ${Math.round(Math.random()*255)}, ${Math.round(Math.random()*255)}, ${Math.random()*5})`;
        let color = colorArray[Math.floor(Math.random() * colorArray.length)];
        let x = Math.random() * (innerWidth - radius * 2) + radius;
        let y = Math.random() * (innerHeight - radius * 2) + radius;
        let dx = (Math.random() - 0.5) * 2;
        let dy = (Math.random() - 0.5) * 2;
        circleArray.push(new Circle(x, y, dx, dy, radius, color));
    }
}


function animate() {
    requestAnimationFrame(animate);
    c.clearRect(0, 0, innerWidth, innerHeight);
    circleArray.forEach(element => {
        element.update();
    });
}

animate();
init();