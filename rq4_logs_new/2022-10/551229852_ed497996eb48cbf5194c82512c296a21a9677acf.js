var player;
var s;
//var b;
function setup() {
  createCanvas(400, 400);
  player = new Ball();
  s = 0.0;
  //b = true;
}

function draw() {
  background(220);
  fill("red");
  stroke("black");
  //figure out how to make it onle work once
  text(s, 200, 200);
  square(370, 0, 30);
  player.display();
  if(keyIsDown(32) /*&& b*/){
    player.move();
  }
}

function Ball(mx, my){
  this.loc = createVector(0,400);
  this.diameter = 40;
  this.speed = createVector(1, -1);
  
  this.move = function(){
    this.loc.add(this.speed);
    this.speed.x = 1.08*this.speed.x;
    this.speed.y = 1.08*this.speed.y;
    if(this.loc.x > 400 || this.loc.y > 400 || this.loc.x < 0 || this.loc.y < 0){
      this.speed = createVector(1, -1);
      this.loc.x = 1;
      this.loc.y = 400;
    }
  };
  this.display = function(){
    fill(random(0,255), random(0,255), random(0,255));
    strokeWeight(random(1,5));
    stroke(random(0,255),random(0,255),random(0,255));
    ellipse(this.loc.x, this.loc.y, this.diameter, this.diameter);
  };
}

function keyReleased(){
  s = 100-4-floor(((sqrt(pow(400-player.loc.x, 2)+pow(player.loc.y, 2))-player.diameter)/565)*100);
  //b = false;
  player = new Ball();
}