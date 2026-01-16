var x = 50;

function setup() {
  createCanvas(400, 400);
}

function draw() {
  background(x, x, x);
  x++;
  if (x > 255) x = 0;

}