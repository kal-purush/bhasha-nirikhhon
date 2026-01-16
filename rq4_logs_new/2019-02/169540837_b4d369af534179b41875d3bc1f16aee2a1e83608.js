class Cell {
  constructor(contents, position) {
    this.pos = position;
    this.r = 50;
    this.c = color(random(255), 0, random(100,255));
    this.stage = 0;
    this.contents = contents;
  }
  
  preload() {
    
  }
  move() {
    var vel = p5.Vector.random2D();
    this.pos.add(vel);
  }

  show() {
    noStroke();
    fill(this.c);
    if (this.stage == 3) {
      ellipse(this.pos.x-10, this.pos.y, this.r*2, this.r*2);
      ellipse(this.pos.x+10, this.pos.y, this.r*2, this.r*2);
    } else if (this.stage == 4) {
      ellipse(this.pos.x-(this.r/2), this.pos.y, this.r*2, this.r*2);
      ellipse(this.pos.x+(this.r/2), this.pos.y, this.r*2, this.r*2);
    } else {
      ellipse(this.pos.x, this.pos.y, this.r*2, this.r*2);
    }
    //ellipse(this.pos.x, this.pos.y, this.r*2, this.r*2);
    
    //interphase = loadImage('assets/Interphase.png');
    if (this.stage == 4) {
      image(this.contents[this.stage], this.pos.x - (this.r/2) - 35, this.pos.y-(this.r/2) - 35, this.r+70, this.r+70)
    } else {
      image(this.contents[this.stage], this.pos.x - (this.r/2) - 10, this.pos.y-(this.r/2) - 10, this.r+20, this.r+20)
    }
    //image(this.contents[this.stage], this.pos.x - (this.r/2) - 10, this.pos.y-(this.r/2) - 10, this.r+20, this.r+20)
  }

  showPhase() {
    fill(204, 101, 192, 127);
    stroke(127, 63, 120);
    rect(mouseX, mouseY - 40, 100, 40);
    fill(255);
    textAlign(CENTER, CENTER)
    text(this.stateTest(), mouseX, mouseY - 40, 100, 40);
  }
  mitosis() {

  }
  stateTest() {
    if (this.stage == 0) {
      return 'STAGE: INTERPHASE'
    } else if (this.stage == 1) {
      return 'STAGE: PROPHASE'
    } else if (this.stage == 2) {
      return 'STAGE: METAPHASE'
    } else if (this.stage == 3) {
      return 'STAGE: ANAPHASE'
    } else if (this.stage == 4) {
      return 'STAGE: TELOPHASE'
    } else {
      this.stage = 0
      return 'STAGE: INTERPHASE'
    }
  }

  isOverCell(x, y) {
    var d = dist(this.pos.x, this.pos.y, x, y);
    if (d < this.r) {
      return true;
    } else {
      return false;
    }
  }

  changeState() {
    if (this.stage < 4)
      this.stage = this.stage + 1
    else {
      this.stage = 0
    }
  }
}