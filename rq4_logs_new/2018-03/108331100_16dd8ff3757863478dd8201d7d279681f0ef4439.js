function windowResized(){
  resizeCanvas(innerWidth,innerHeight);
}

function mouseMoved(){

}

var can;

var gridSize = 20;

var grid = null;

var cursor = null;

function setup(){

  can = createCanvas(innerWidth,innerHeight);

  can.parent("editor_canvas")

  grid = new Grid();

  cursor = new Cursor();

}

function draw(){

  noStroke();

  background(51);

  grid.update();
  grid.draw();

  cursor.update();
  cursor.draw();


}

class Wall {

  constructor(){
    this.startPos = null;
    this.endPos = null
    this.direction = 0; // 1 === horizontal, 2 === vertical
  }

  start(s){
    this.startPos = createVector(s.x,s.y);
  }

  end(e){
    this.endPos = createVector(e.x,e.y);
  }

  single(){
    if(this.endPos === null) return false
    return this.startPos.x === this.endPos.x && this.startPos.y === this.endPos.y
  }

  alignment(){

    if(this.startPos === null) return true;

    let c = cursor.get();
    return this.startPos.x === c.x || this.startPos.y === c.y
  }

  update(){


  }

  draw(){

    fill(255,255,255);

    if(this.startPos != null){

      rect(this.startPos.x,this.startPos.y,gridSize,gridSize);

    }

    if(this.startPos != null && this.endPos === null && this.alignment()){
      fill(255,255,255,100);

      rect(
        this.startPos.x,
        this.startPos.y,
        (cursor.get().x-this.startPos.x)+gridSize,
        (cursor.get().y-this.startPos.y)+gridSize
      )
    }

    if(this.endPos != null)
      rect(this.endPos.x,this.endPos.y,gridSize,gridSize);

    if(this.startPos != null && this.endPos != null){
      rect(
        this.startPos.x,
        this.startPos.y,
        (this.endPos.x-this.startPos.x)+gridSize,
        (this.endPos.y-this.startPos.y)+gridSize
      )

    }

  }

}

class Cursor {

  constructor(){
    this.pos = createVector(
      floor( mouseX / gridSize ) * gridSize,
      floor( mouseY / gridSize ) * gridSize
    )

    this.color = color(0, 255, 0);

  }

  get(){
    return this.pos;
  }

  update(){

    this.pos.x = floor( mouseX / gridSize ) * gridSize;
    this.pos.y = floor( mouseY / gridSize ) * gridSize;

    if(grid.newWall != null){

      let ws = grid.newWall.startPos;
      let we = grid.newWall.endPos;

      if(grid.newWall.alignment() && !grid.newWall.single()){
        this.color = color(0,255,0);
      } else {
        this.color = color(255,0,0);
      }

    }


  }

  draw(){

    fill(this.color)
    rect(
      this.pos.x,
      this.pos.y,
      gridSize,
      gridSize
    );


    if(grid.newWall != null){

      let ws = grid.newWall.startPos;
      let we = grid.newWall.endPos;

      fill(255,255,0);

      if(ws != null){
        text("Start: " + ws.x + ":" + ws.y, this.pos.x,this.pos.y-20);
        text("End  : " + this.pos.x + ":"+this.pos.y, this.pos.x,this.pos.y);

      }

    }

  }

}

class Grid {

  constructor(){

    this.newWall = null;

    this.mouseDown = false;

    this.drawing = false;

    this.walls = [];

  }

  update(){

    if(this.newWall != null) this.newWall.update();

    if(mouseIsPressed && !this.mouseDown){
      this.mouseDown = true;
      this.drawing = true;
      this.newWall = new Wall();
      this.newWall.start(cursor.get());
    }

    if(!mouseIsPressed && this.mouseDown){

      this.mouseDown = false;
      this.newWall.end(cursor.get());

      if(this.newWall.alignment() && !this.newWall.single()){
        this.walls.push(this.newWall);
      }

      this.newWall = null;

    }

  }

  draw(){

    strokeWeight(0.5);
    stroke(255);

    for(let h = 0 ; h < (width / gridSize) ; h++){
      line((h*gridSize),0,(h*gridSize),height);
    }

    for(let v = 0 ; v < height / gridSize ; v++){
      line(0,(v*gridSize),width,(v*gridSize));
    }

    noStroke();

    if(mouseIsPressed && this.mouseDown){
      fill(100,100,100,150);
      rect(cursor.get().x,0,gridSize,height);
      rect(0,cursor.get().y,width,gridSize);
    }

    if(this.newWall != null) this.newWall.draw();

    for(let wall in this.walls){
      this.walls[wall].draw();
    }

  }

}